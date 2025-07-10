"""Validation utilities for FinOps data models."""

from typing import List, Dict, Any
import structlog
from .discovery_models import DiscoveryInfo, ClusterInfo

logger = structlog.get_logger(__name__)


def validate_discovery_info(discovery: DiscoveryInfo) -> List[str]:
    """Validate discovery info and return validation errors."""
    errors = []
    
    # Basic validation
    if not discovery.clusters:
        errors.append("No clusters found in discovery")
    
    # Validate each cluster
    for cluster in discovery.clusters:
        cluster_errors = validate_cluster_info(cluster)
        errors.extend([f"Cluster {cluster.name}: {error}" for error in cluster_errors])
    
    return errors


def validate_cluster_info(cluster: ClusterInfo) -> List[str]:
    """Validate cluster information."""
    errors = []
    
    if not cluster.name:
        errors.append("Cluster name is required")
    
    if not cluster.node_pools:
        errors.append("At least one node pool is required")
    
    # Validate node pools
    for node_pool in cluster.node_pools:
        if node_pool.count < 0:
            errors.append(f"Node pool {node_pool.name} has negative count")
        
        if node_pool.auto_scaling_enabled and (not node_pool.min_count or not node_pool.max_count):
            errors.append(f"Node pool {node_pool.name} has auto-scaling enabled but missing min/max count")
    
    return errors


def validate_cost_data(cost_data: Dict[str, Any]) -> List[str]:
    """Validate cost data structure."""
    errors = []
    
    required_fields = ['total', 'currency']
    for field in required_fields:
        if field not in cost_data:
            errors.append(f"Missing required cost field: {field}")
    
    if 'total' in cost_data and cost_data['total'] < 0:
        errors.append("Total cost cannot be negative")
    
    return errors