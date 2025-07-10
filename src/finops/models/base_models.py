"""Base models for common FinOps data structures."""

from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum
import uuid


class FinOpsBaseModel(BaseModel):
    """Base model for all FinOps data structures."""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }


class OptimizationRecommendation(FinOpsBaseModel):
    """Model for optimization recommendations."""
    
    title: str
    description: str
    category: str  # cost, performance, security, etc.
    priority: str  # high, medium, low
    potential_savings: float = 0.0
    implementation_effort: str  # low, medium, high
    risk_level: str  # low, medium, high
    affected_resources: int = 0
    detailed_steps: list = Field(default_factory=list)
    prerequisites: list = Field(default_factory=list)
    estimated_implementation_time: str = "Unknown"