"""
SQLAlchemy ORM Models for database tables
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, BigInteger
from datetime import datetime
from database import Base

class Dataset(Base):
    """
    Represents a dataset stored in the system
    Tracks current tier, access patterns, and metadata
    """
    __tablename__ = "datasets"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), unique=True, index=True, nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    current_tier = Column(String(10), nullable=False)  # hot, warm, cold
    access_freq_7d = Column(Integer, default=0)  # Access count in last 7 days
    avg_latency_ms = Column(Float, default=0.0)  # Average access latency
    last_accessed = Column(DateTime, default=datetime.utcnow)
    access_window_start = Column(DateTime, default=datetime.utcnow)  # For 7-day windowing
    created_at = Column(DateTime, default=datetime.utcnow)
    version = Column(Integer, default=1)  # Version for data consistency
    
    def __repr__(self):
        return f"<Dataset(name={self.name}, tier={self.current_tier}, size={self.size_bytes})>"

class Movement(Base):
    """
    Records data movement between tiers
    Tracks status, timing, and reason for movement
    """
    __tablename__ = "movements"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, nullable=False, index=True)
    from_tier = Column(String(10), nullable=False)
    to_tier = Column(String(10), nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    reason = Column(String(100))  # manual, ml_prediction, policy
    status = Column(String(20), default="pending")  # pending, in_progress, completed, failed
    initiated_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    
    def __repr__(self):
        return f"<Movement(dataset_id={self.dataset_id}, {self.from_tier}->{self.to_tier}, status={self.status})>"

class Metric(Base):
    """
    Time-series metrics for system monitoring
    Records costs, latencies, and tier distributions
    """
    __tablename__ = "metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    total_datasets = Column(Integer, default=0)
    hot_count = Column(Integer, default=0)
    warm_count = Column(Integer, default=0)
    cold_count = Column(Integer, default=0)
    hot_size_bytes = Column(BigInteger, default=0)
    warm_size_bytes = Column(BigInteger, default=0)
    cold_size_bytes = Column(BigInteger, default=0)
    estimated_cost_usd = Column(Float, default=0.0)
    avg_latency_ms = Column(Float, default=0.0)
    
    def __repr__(self):
        return f"<Metric(timestamp={self.timestamp}, cost=${self.estimated_cost_usd})>"

