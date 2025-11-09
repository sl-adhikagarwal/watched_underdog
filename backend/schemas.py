"""
Pydantic schemas for request/response validation
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List
from datetime import datetime

class DatasetCreate(BaseModel):
    """Request schema for creating a new dataset"""
    name: str = Field(..., description="Unique dataset name")
    size_bytes: int = Field(..., gt=0, description="Dataset size in bytes")
    initial_tier: str = Field(default="warm", pattern="^(hot|warm|cold)$")

class DatasetResponse(BaseModel):
    """Response schema for dataset information"""
    id: int
    name: str
    size_bytes: int
    current_tier: str
    access_freq_7d: int
    avg_latency_ms: float
    last_accessed: datetime
    created_at: datetime
    version: int
    
    model_config = ConfigDict(from_attributes=True)

class DecisionRequest(BaseModel):
    """Request schema for tier placement decision"""
    dataset_id: int = Field(..., description="ID of dataset to evaluate")
    auto_execute: bool = Field(default=True, description="Automatically execute tier change")

class DecisionResponse(BaseModel):
    """Response schema for tier decision"""
    dataset_id: int
    dataset_name: str
    current_tier: str
    recommended_tier: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    should_move: bool
    executed: bool

class MovementResponse(BaseModel):
    """Response schema for movement history"""
    id: int
    dataset_id: int
    dataset_name: str
    from_tier: str
    to_tier: str
    size_bytes: int
    reason: Optional[str]
    status: str
    initiated_at: datetime
    completed_at: Optional[datetime]
    error_message: Optional[str] = None

class TierDistribution(BaseModel):
    """Tier distribution stats"""
    tier: str
    count: int
    size_bytes: int

class MetricsResponse(BaseModel):
    """Response schema for system metrics"""
    total_datasets: int
    total_size_bytes: int
    tier_distribution: List[TierDistribution]
    estimated_cost_usd: float
    total_movements: int
    successful_movements: int
    failed_movements: int


