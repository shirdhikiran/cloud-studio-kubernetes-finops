# src/finops/cli_phase2.py
"""
Phase 2 Analytics CLI - Process Phase 1 data and generate insights
"""

import asyncio
import click
import json
from pathlib import Path
import structlog
from datetime import datetime, timezone

from finops.analytics.analytics_engine import AnalyticsEngine
from finops.config.settings import Settings
from finops.core.utils import setup_logging


logger = structlog.get_logger(__name__)


@click.command()
@click.option('--input', '-i', required=True, help='Input JSON file from Phase 1 discovery')
@click.option('--output', '-o', default='./data/phase2_analytics_results.json', help='Output JSON file path')
@click.option('--format', '-f', type=click.Choice(['json', 'html', 'pdf']), default='json', help='Output format')
@click.option('--cost-threshold', default=1000.0, help='High cost threshold in USD (default: 1000)')
@click.option('--efficiency-target', default=70.0, help='Efficiency target percentage (default: 70)')
@click.option('--waste-threshold', default=20.0, help='Waste threshold percentage (default: 20)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.option('--insights-only', is_flag=True, help='Generate only key insights (faster)')
@click.option('--generate-report', is_flag=True, help='Generate comprehensive HTML report')
def analyze(input, output, format, cost_threshold, efficiency_target, waste_threshold, 
           verbose, debug, insights_only, generate_report):
    """
    Phase 2 Analytics: Process Phase 1 discovery data to generate actionable FinOps insights.
    
    This command performs comprehensive analytics on Phase 1 discovery data:
    - Cost analysis and optimization opportunities
    - Resource utilization analysis and right-sizing recommendations  
    - Waste identification and quantification
    - Forecasting and capacity planning
    - Benchmarking against industry standards
    - Actionable insights with priority rankings
    
    Input: JSON file from Phase 1 discovery (finops-discover command)
    Output: Comprehensive analytics report with recommendations
    
    Example:
        python -m finops.cli_phase2 -i phase1_data.json -o analytics_report.json --generate-report
    """
    
    async def run_phase2_analytics():
        try:
            # Setup logging
            log_level = "DEBUG" if debug else "INFO"
            setup_logging(log_level=log_level)
            
            # Validate input file
            input_path = Path(input)
            if not input_path.exists():
                click.echo(f"❌ Error: Input file not found: {input_path}")
                return 1
            
            # Load Phase 1 discovery data
            try:
                with open(input_path, 'r') as f:
                    phase1_data = json.load(f)
                
                # Validate Phase 1 data structure
                if "discovery_data" not in phase1_data:
                    click.echo("❌ Error: Invalid Phase 1 data format - missing 'discovery_data'")
                    return 1
                
                discovery_data = phase1_data["discovery_data"]
                
                if verbose:
                    click.echo("🔍 Phase 2: Analytics - Generating Insights")
                    click.echo(f"📁 Input file: {input_path}")
                    click.echo(f"📊 Analysis Parameters:")
                    click.echo(f"   • Cost threshold: ${cost_threshold}")
                    click.echo(f"   • Efficiency target: {efficiency_target}%")
                    click.echo(f"   • Waste threshold: {waste_threshold}%")
                    click.echo(f"📈 Analytics Modules:")
                    click.echo(f"   • Cost Analysis & Attribution")
                    click.echo(f"   • Utilization Analysis & Right-sizing")
                    click.echo(f"   • Waste Detection & Quantification")
                    click.echo(f"   • Industry Benchmarking")
                
            except json.JSONDecodeError as e:
                click.echo(f"❌ Error: Invalid JSON in input file: {e}")
                return 1
            
            # Initialize analytics engine
            analytics_config = {
                'high_cost_threshold': cost_threshold,
                'efficiency_target': efficiency_target,
                'waste_threshold': waste_threshold,
                'cpu_target_utilization': efficiency_target,
                'memory_target_utilization': efficiency_target,
                'low_utilization_threshold': 100 - efficiency_target,
                'insights_only': insights_only
            }
            
            analytics_engine = AnalyticsEngine(analytics_config)
            
            if verbose:
                click.echo("🚀 Starting Phase 2 analytics processing...")
            
            # Run comprehensive analytics
            analytics_report = await analytics_engine.analyze_discovery_data(discovery_data)
            
            # Prepare output
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate output based on format
            if format == 'json':
                # Export to JSON
                report_dict = analytics_engine.export_report_to_dict(analytics_report)
                
                with open(output_path, 'w') as f:
                    json.dump(report_dict, f, indent=2, default=str)
                
                click.echo(f"✅ Phase 2 Analytics completed!")
                click.echo(f"📁 Analytics report saved to: {output_path}")
                
            elif format == 'html' or generate_report:
                # Generate HTML report
                html_output_path = output_path.with_suffix('.html')
                await generate_html_report(analytics_report, html_output_path)
                
                # Also save JSON for programmatic access
                json_output_path = output_path.with_suffix('.json')
                report_dict = analytics_engine.export_report_to_dict(analytics_report)
                with open(json_output_path, 'w') as f:
                    json.dump(report_dict, f, indent=2, default=str)
                
                click.echo(f"✅ Phase 2 Analytics completed!")
                click.echo(f"📊 HTML report: {html_output_path}")
                click.echo(f"📁 JSON data: {json_output_path}")
            
            # Print executive summary
            executive_summary = analytics_report.executive_summary
            
            click.echo("\n" + "="*60)
            click.echo("📊 EXECUTIVE SUMMARY")
            click.echo("="*60)
            
            click.echo(f"🏗️  Cluster: {executive_summary.get('cluster_name', 'Unknown')}")
            click.echo(f"📅 Analysis Date: {executive_summary.get('analysis_date', 'Unknown')}")
            click.echo(f"💰 Monthly Cost: ${executive_summary.get('total_monthly_cost', 0):.2f}")
            click.echo(f"💡 Potential Savings: ${executive_summary.get('total_potential_savings', 0):.2f} ({executive_summary.get('savings_percentage', 0):.1f}%)")
            click.echo(f"⚡ Efficiency Score: {executive_summary.get('efficiency_score', 0):.1f}%")
            click.echo(f"🏥 Health Status: {executive_summary.get('health_status', 'Unknown').title()}")
            
            # Insights summary
            insights_summary = executive_summary.get('insights_summary', {})
            click.echo(f"\n🔍 Insights Generated:")
            click.echo(f"   🔴 Critical: {insights_summary.get('critical_insights', 0)}")
            click.echo(f"   🟡 High Priority: {insights_summary.get('high_priority_insights', 0)}")
            click.echo(f"   🟢 Medium Priority: {insights_summary.get('medium_priority_insights', 0)}")
            click.echo(f"   ℹ️  Low Priority: {insights_summary.get('low_priority_insights', 0)}")
            
            # Top opportunities
            top_opportunities = executive_summary.get('top_opportunities', [])
            if top_opportunities:
                click.echo(f"\n🎯 Top Optimization Opportunities:")
                for i, opp in enumerate(top_opportunities[:3], 1):
                    click.echo(f"   {i}. {opp['title']} - ${opp['potential_savings']:.2f} savings")
            
            # Key metrics
            key_metrics = analytics_report.key_metrics
            
            click.echo(f"\n📈 Key Metrics:")
            cost_metrics = key_metrics.get('cost_metrics', {})
            util_metrics = key_metrics.get('utilization_metrics', {})
            
            click.echo(f"   💰 Cost per Pod: ${cost_metrics.get('cost_per_pod', 0):.2f}")
            click.echo(f"   💰 Cost per CPU Hour: ${cost_metrics.get('cost_per_cpu_hour', 0):.4f}")
            click.echo(f"   📊 CPU Utilization: {util_metrics.get('cpu_utilization', 0):.1f}%")
            click.echo(f"   📊 Memory Utilization: {util_metrics.get('memory_utilization', 0):.1f}%")
            
            efficiency_metrics = key_metrics.get('efficiency_metrics', {})
            savings_metrics = key_metrics.get('savings_metrics', {})
            
            click.echo(f"   ⚡ Resource Efficiency: {efficiency_metrics.get('resource_efficiency_score', 0):.1f}%")
            click.echo(f"   🗑️  Waste Percentage: {efficiency_metrics.get('waste_percentage', 0):.1f}%")
            click.echo(f"   💡 Quick Wins: {savings_metrics.get('quick_wins', 0)} opportunities")
            
            # Priority actions
            priority_actions = analytics_report.priority_actions
            if priority_actions:
                click.echo(f"\n🎯 Recommended Actions:")
                for i, action in enumerate(priority_actions[:5], 1):
                    priority_emoji = "🔴" if action['priority'] == 'high' else "🟡"
                    click.echo(f"   {priority_emoji} {i}. {action['title']} - ${action['estimated_savings']:.2f} savings")
                    click.echo(f"      Timeline: {action.get('timeline', 'TBD')} | Effort: {action.get('effort_level', 'Unknown')}")
            
            # Analysis completeness
            detailed_analysis = analytics_report
            analysis_modules = {
                'Cost Analysis': bool(detailed_analysis.cost_analysis),
                'Utilization Analysis': bool(detailed_analysis.utilization_analysis),
                'Waste Analysis': bool(detailed_analysis.waste_analysis),
                'Right-sizing Analysis': bool(detailed_analysis.rightsizing_analysis),
                'Benchmarking Analysis': bool(detailed_analysis.benchmarking_analysis)
            }
            
            click.echo(f"\n🔧 Analysis Modules Completed:")
            for module, completed in analysis_modules.items():
                status = "✅" if completed else "❌"
                click.echo(f"   {status} {module}")
            
            # Next steps
            click.echo(f"\n🚀 Next Steps:")
            if executive_summary.get('savings_percentage', 0) > 20:
                click.echo(f"   1. Review high-impact optimization opportunities")
                click.echo(f"   2. Implement quick wins for immediate savings")
                click.echo(f"   3. Plan medium-term right-sizing initiatives")
            elif executive_summary.get('efficiency_score', 0) < 60:
                click.echo(f"   1. Focus on utilization improvements")
                click.echo(f"   2. Implement resource governance policies")
                click.echo(f"   3. Set up monitoring and alerting")
            else:
                click.echo(f"   1. Monitor and maintain current efficiency levels")
                click.echo(f"   2. Implement predictive scaling based on forecasts")
                click.echo(f"   3. Regular optimization reviews")
            
            if generate_report:
                click.echo(f"\n📊 Open the HTML report for detailed analysis and visualizations")
            
            return 0
            
        except Exception as e:
            click.echo(f"❌ Phase 2 Analytics failed: {e}")
            if debug:
                import traceback
                click.echo(traceback.format_exc())
            return 1
    
    exit_code = asyncio.run(run_phase2_analytics())
    exit(exit_code)


async def generate_html_report(analytics_report, output_path: Path):
    """Generate comprehensive HTML report"""
    
    html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FinOps Analytics Report - {cluster_name}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 8px 8px 0 0; }}
        .header h1 {{ margin: 0; font-size: 2.5em; }}
        .header p {{ margin: 5px 0 0 0; opacity: 0.9; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; padding: 30px; }}
        .metric-card {{ background: #f8fafc; border-left: 4px solid #3b82f6; padding: 20px; border-radius: 4px; }}
        .metric-card h3 {{ margin: 0 0 10px 0; color: #374151; font-size: 0.9em; text-transform: uppercase; letter-spacing: 0.5px; }}
        .metric-card .value {{ font-size: 2em; font-weight: bold; color: #1f2937; }}
        .metric-card .change {{ font-size: 0.9em; margin-top: 5px; }}
        .positive {{ color: #10b981; }}
        .negative {{ color: #ef4444; }}
        .section {{ padding: 30px; border-top: 1px solid #e5e7eb; }}
        .section h2 {{ color: #374151; margin-bottom: 20px; }}
        .insights-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
        .insight-card {{ border: 1px solid #e5e7eb; border-radius: 8px; padding: 20px; }}
        .insight-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
        .severity-critical {{ border-left: 4px solid #dc2626; }}
        .severity-high {{ border-left: 4px solid #f59e0b; }}
        .severity-medium {{ border-left: 4px solid #3b82f6; }}
        .severity-low {{ border-left: 4px solid #10b981; }}
        .badge {{ padding: 4px 8px; border-radius: 4px; font-size: 0.8em; font-weight: 500; }}
        .badge-critical {{ background: #fef2f2; color: #dc2626; }}
        .badge-high {{ background: #fffbeb; color: #f59e0b; }}
        .badge-medium {{ background: #eff6ff; color: #3b82f6; }}
        .badge-low {{ background: #f0fdf4; color: #10b981; }}
        .recommendations {{ background: #f8fafc; padding: 20px; border-radius: 8px; margin-top: 20px; }}
        .recommendation-item {{ padding: 10px 0; border-bottom: 1px solid #e5e7eb; }}
        .recommendation-item:last-child {{ border-bottom: none; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; }}
        th {{ background: #f8fafc; font-weight: 600; }}
        .status-good {{ color: #10b981; }}
        .status-warning {{ color: #f59e0b; }}
        .status-critical {{ color: #dc2626; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>FinOps Analytics Report</h1>
            <p>{cluster_name} • {analysis_date}</p>
        </div>
        
        <div class="summary">
            <div class="metric-card">
                <h3>Monthly Cost</h3>
                <div class="value">${total_monthly_cost:,.2f}</div>
                <div class="change">Current spend</div>
            </div>
            <div class="metric-card">
                <h3>Potential Savings</h3>
                <div class="value">${total_potential_savings:,.2f}</div>
                <div class="change positive">({savings_percentage:.1f}% reduction)</div>
            </div>
            <div class="metric-card">
                <h3>Efficiency Score</h3>
                <div class="value">{efficiency_score:.1f}%</div>
                <div class="change">Overall efficiency</div>
            </div>
            <div class="metric-card">
                <h3>Health Status</h3>
                <div class="value">{health_status}</div>
                <div class="change">Cluster health</div>
            </div>
        </div>
        
        <div class="section">
            <h2>🔍 Key Insights</h2>
            <div class="insights-grid">
                {insights_html}
            </div>
        </div>
        
        <div class="section">
            <h2>🎯 Priority Actions</h2>
            <div class="recommendations">
                {actions_html}
            </div>
        </div>
        
        <div class="section">
            <h2>📊 Detailed Metrics</h2>
            <table>
                <tr>
                    <th>Metric</th>
                    <th>Current Value</th>
                    <th>Target</th>
                    <th>Status</th>
                </tr>
                {metrics_table_html}
            </table>
        </div>
        
        <div class="section">
            <h2>💡 Recommendations Summary</h2>
            <div class="recommendations">
                {recommendations_html}
            </div>
        </div>
    </div>
</body>
</html>
"""
    
    # Extract data for template
    executive_summary = analytics_report.executive_summary
    
    # Generate insights HTML
    insights_html = ""
    for insight in analytics_report.insights[:12]:  # Top 12 insights
        severity_class = f"severity-{insight.severity.value}"
        badge_class = f"badge-{insight.severity.value}"
        
        insights_html += f"""
        <div class="insight-card {severity_class}">
            <div class="insight-header">
                <strong>{insight.title}</strong>
                <span class="badge {badge_class}">{insight.severity.value.upper()}</span>
            </div>
            <p>{insight.description}</p>
            <div style="margin-top: 10px;">
                <strong>Potential Savings:</strong> ${insight.potential_savings:.2f}
                <span style="margin-left: 20px;"><strong>Confidence:</strong> {insight.confidence:.0f}%</span>
            </div>
        </div>
        """
    
    # Generate actions HTML
    actions_html = ""
    for action in analytics_report.priority_actions[:10]:  # Top 10 actions
        priority_color = "#dc2626" if action['priority'] == 'high' else "#f59e0b"
        actions_html += f"""
        <div class="recommendation-item">
            <div style="display: flex; justify-content: between; align-items: center;">
                <div>
                    <strong style="color: {priority_color};">{action['title']}</strong>
                    <p style="margin: 5px 0;">{action.get('description', '')}</p>
                    <small>Timeline: {action.get('timeline', 'TBD')} | Effort: {action.get('effort_level', 'Unknown')}</small>
                </div>
                <div style="text-align: right;">
                    <strong>${action['estimated_savings']:.2f}</strong><br>
                    <small>savings</small>
                </div>
            </div>
        </div>
        """
    
    # Generate metrics table HTML
    metrics_table_html = ""
    key_metrics = analytics_report.key_metrics
    
    metrics_data = [
        ("CPU Utilization", f"{key_metrics.get('utilization_metrics', {}).get('cpu_utilization', 0):.1f}%", "70%", 
         key_metrics.get('utilization_metrics', {}).get('cpu_utilization', 0)),
        ("Memory Utilization", f"{key_metrics.get('utilization_metrics', {}).get('memory_utilization', 0):.1f}%", "70%",
         key_metrics.get('utilization_metrics', {}).get('memory_utilization', 0)),
        ("Resource Efficiency", f"{key_metrics.get('efficiency_metrics', {}).get('resource_efficiency_score', 0):.1f}%", "70%",
         key_metrics.get('efficiency_metrics', {}).get('resource_efficiency_score', 0)),
        ("Waste Percentage", f"{key_metrics.get('efficiency_metrics', {}).get('waste_percentage', 0):.1f}%", "<20%",
         key_metrics.get('efficiency_metrics', {}).get('waste_percentage', 0))
    ]
    
    for metric, current, target, value in metrics_data:
        if metric == "Waste Percentage":
            status = "status-good" if value < 20 else "status-warning" if value < 30 else "status-critical"
            status_text = "Good" if value < 20 else "Warning" if value < 30 else "Critical"
        else:
            status = "status-good" if value >= 70 else "status-warning" if value >= 50 else "status-critical"
            status_text = "Good" if value >= 70 else "Warning" if value >= 50 else "Critical"
        
        metrics_table_html += f"""
        <tr>
            <td>{metric}</td>
            <td>{current}</td>
            <td>{target}</td>
            <td class="{status}">{status_text}</td>
        </tr>
        """
    
    # Generate recommendations HTML
    recommendations_html = ""
    all_recommendations = (
        analytics_report.cost_analysis.get('recommendations', []) +
        analytics_report.utilization_analysis.get('recommendations', []) +
        analytics_report.waste_analysis.get('recommendations', [])
    )
    
    for i, rec in enumerate(all_recommendations[:10], 1):
        recommendations_html += f"""
        <div class="recommendation-item">
            <strong>{i}.</strong> {rec}
        </div>
        """
    
    # Fill template
    html_content = html_template.format(
        cluster_name=executive_summary.get('cluster_name', 'Unknown'),
        analysis_date=executive_summary.get('analysis_date', 'Unknown'),
        total_monthly_cost=executive_summary.get('total_monthly_cost', 0),
        total_potential_savings=executive_summary.get('total_potential_savings', 0),
        savings_percentage=executive_summary.get('savings_percentage', 0),
        efficiency_score=executive_summary.get('efficiency_score', 0),
        health_status=executive_summary.get('health_status', 'Unknown').title(),
        insights_html=insights_html,
        actions_html=actions_html,
        metrics_table_html=metrics_table_html,
        recommendations_html=recommendations_html
    )
    
    # Write HTML file
    with open(output_path, 'w') as f:
        f.write(html_content)


if __name__ == '__main__':
    analyze()