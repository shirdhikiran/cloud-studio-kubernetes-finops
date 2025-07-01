"""Metrics collection discovery service."""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta, timezone
import structlog

from finops.discovery.base import BaseDiscoveryService
from finops.clients.azure.monitor_client import MonitorClient

logger = structlog.get_logger(__name__)


class MetricsCollectionService(BaseDiscoveryService):
    """Service for collecting metrics from Azure Monitor."""
    
    def __init__(self, monitor_client: MonitorClient, config: Dict[str, Any]):
        super().__init__(monitor_client, config)
        self.cluster_resource_id = config.get("cluster_resource_id")
        self.metrics_time_range_hours = config.get("metrics_time_range_hours", 24)
        self.include_node_metrics = config.get("include_node_metrics", True)
        self.include_pod_metrics = config.get("include_pod_metrics", False)  # Requires Log Analytics
    
    async def discover(self) -> List[Dict[str, Any]]:
        """Collect metrics data."""
        self.logger.info("Starting metrics collection")
        
        if not self.client.is_connected:
            await self.client.connect()
        
        if not self.cluster_resource_id:
            self.logger.warning("No cluster resource ID provided, skipping metrics collection")
            return []
        
        metrics_data = []
        
        try:
            # Calculate time range
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(hours=self.metrics_time_range_hours)
            
            # Collect cluster-level metrics
            cluster_metrics = await self.client.get_cluster_metrics(
                cluster_resource_id=self.cluster_resource_id,
                start_time=start_time,
                end_time=end_time
            )
            
            if cluster_metrics:
                metrics_data.append({
                    'type': 'cluster_metrics',
                    'resource_id': self.cluster_resource_id,
                    'data': cluster_metrics,
                    'collection_start': start_time.isoformat(),
                    'collection_end': end_time.isoformat(),
                    'time_range_hours': self.metrics_time_range_hours
                })
            
            # Collect node-specific metrics if requested
            if self.include_node_metrics:
                # This would require getting node names first
                # For now, we'll create a placeholder
                node_metrics_summary = {
                    'total_nodes_monitored': 0,
                    'metrics_available': cluster_metrics is not None,
                    'node_level_detail': 'requires_node_enumeration'
                }
                
                metrics_data.append({
                    'type': 'node_metrics_summary',
                    'resource_id': self.cluster_resource_id,
                    'data': node_metrics_summary
                })
            
        except Exception as e:
            self.logger.error("Failed to collect metrics", error=str(e))
            raise
        
        self.logger.info(f"Collected metrics data for {len(metrics_data)} categories")
        return metrics_data
    
    def get_discovery_type(self) -> str:
        """Get discovery type."""
        return "metrics_collection"