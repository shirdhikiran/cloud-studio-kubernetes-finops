"""Core data models used across the platform."""

from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum
import uuid


class ResourceStatus(str, Enum):
    """Resource status enumeration."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    FAILED = "failed"
    UNKNOWN = "unknown"


class MetricType(str, Enum):
    """Metric type enumeration."""
    CPU = "cpu"
    MEMORY = "memory"
    STORAGE = "storage"
    NETWORK = "network"
    COST = "cost"


class BaseFinOpsModel(BaseModel):
    """Base model with common fields."""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }
    
    def dict_without_none(self) -> Dict[str, Any]:
        """Return dict representation excluding None values."""
        return {k: v for k, v in self.model_dump().items() if v is not None}


class MetricDataPoint(BaseModel):
    """A single metric data point."""
    
    timestamp: datetime
    value: float
    unit: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResourceMetrics(BaseModel):
    """Resource metrics container."""
    
    resource_id: str
    resource_type: str
    metrics: Dict[str, List[MetricDataPoint]] = Field(default_factory=dict)
    collection_time: datetime = Field(default_factory=datetime.utcnow)
    
    def add_metric(self, metric_type: str, data_points: List[MetricDataPoint]) -> None:
        """Add metric data points."""
        self.metrics[metric_type] = data_points
    
    def get_latest_value(self, metric_type: str) -> Optional[float]:
        """Get the latest value for a metric type."""
        if metric_type in self.metrics and self.metrics[metric_type]:
            return self.metrics[metric_type][-1].value
        return None


class CostData(BaseModel):
    """Cost data model."""
    
    resource_id: str
    resource_type: str
    total_cost: float
    currency: str = "USD"
    cost_breakdown: Dict[str, float] = Field(default_factory=dict)
    billing_period_start: datetime
    billing_period_end: datetime
    
    @validator('total_cost')
    def validate_cost(cls, v):
        if v < 0:
            raise ValueError("Cost cannot be negative")
        return v