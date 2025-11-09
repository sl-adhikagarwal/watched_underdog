"""
FastAPI Backend for Intelligent Cloud Storage System
Handles dataset management, tier decisions, and ML predictions
PRODUCTION-READY with fault tolerance and proper resource management
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session
import os
from datetime import datetime, timedelta
from typing import List, Optional
import logging

from database import init_db, get_db
from models import Dataset, Movement, Metric
from schemas import (
    DatasetCreate, DatasetResponse, MovementResponse,
    DecisionRequest, DecisionResponse, MetricsResponse,
    TierDistribution
)
from ml_engine import MLEngine
from kafka_client import KafkaProducer as StorageKafkaProducer
from migrations import run_migrations, get_migration_status

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global state with fault tolerance
ml_engine = MLEngine()
kafka_producer = None
ml_available = False
kafka_available = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown events with fault-tolerant initialization
    Services degrade gracefully if dependencies aren't ready
    """
    global kafka_producer, ml_available, kafka_available
    
    # Startup
    logger.info("Starting Intelligent Cloud Storage Backend")
    
    # Initialize database (critical - must succeed)
    try:
        init_db()
        logger.info("Database initialized")
        
        # Run migrations
        run_migrations()
        # TODO: Evaluate moving to Alembic-managed migrations so existing volumes survive schema changes
        logger.info("Database migrations applied")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise  # Can't continue without database
    
    # Initialize Kafka producer (non-critical - can fail)
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    try:
        kafka_producer = StorageKafkaProducer(kafka_bootstrap)
        kafka_available = True
        logger.info("Kafka producer initialized")
    except Exception as e:
        logger.warning(f"Kafka unavailable, events will not be published: {e}")
        kafka_available = False
    
    # Train initial ML model (non-critical - can fall back to rules)
    try:
        ml_engine.train_model()
        ml_available = True
        logger.info("ML model trained successfully")
    except Exception as e:
        logger.warning(f"ML training failed, using rule-based predictions: {e}")
        ml_available = False
    
    logger.info("Backend ready")
    
    yield
    
    # Shutdown
    if kafka_producer:
        try:
            kafka_producer.close()
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
    logger.info("Backend shutting down")

app = FastAPI(
    title="Intelligent Cloud Storage API",
    description="Dynamic multi-tier storage with ML-powered optimization",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    """Health check endpoint with service status"""
    migration_status = get_migration_status()
    
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "database": True,
            "kafka": kafka_available,
            "ml_model": ml_available,
            "migrations": migration_status.get("status", "unknown")
        }
    }

@app.get("/datasets", response_model=List[DatasetResponse])
async def list_datasets(tier: Optional[str] = None, db: Session = Depends(get_db)):
    """List all datasets, optionally filtered by tier"""
    query = db.query(Dataset)
    
    if tier:
        query = query.filter(Dataset.current_tier == tier)
    
    datasets = query.all()
    return [DatasetResponse.from_orm(ds) for ds in datasets]

@app.post("/datasets", response_model=DatasetResponse)
async def create_dataset(dataset: DatasetCreate, db: Session = Depends(get_db)):
    """Create a new dataset entry"""
    # Check if dataset already exists
    existing = db.query(Dataset).filter(Dataset.name == dataset.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Dataset already exists")
    
    # Create new dataset
    db_dataset = Dataset(
        name=dataset.name,
        size_bytes=dataset.size_bytes,
        current_tier=dataset.initial_tier,
        access_freq_7d=0,
        avg_latency_ms=0.0,
        last_accessed=datetime.utcnow(),
        version=1,
        access_window_start=datetime.utcnow()  # Track window start
    )
    
    db.add(db_dataset)
    db.commit()
    db.refresh(db_dataset)
    
    return DatasetResponse.from_orm(db_dataset)

@app.get("/datasets/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: int, db: Session = Depends(get_db)):
    """Get a specific dataset by ID"""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    return DatasetResponse.from_orm(dataset)

def _reset_access_window_if_needed(dataset: Dataset) -> None:
    """Reset 7-day access window if it's expired (null-safe for migrations)"""
    # Handle null access_window_start for legacy data
    if dataset.access_window_start is None:
        dataset.access_window_start = datetime.utcnow()
        dataset.access_freq_7d = 0
        return
    
    window_age = datetime.utcnow() - dataset.access_window_start
    if window_age > timedelta(days=7):
        dataset.access_freq_7d = 0
        dataset.access_window_start = datetime.utcnow()

@app.post("/decide", response_model=DecisionResponse)
async def decide_tier_placement(
    request: DecisionRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """
    Decide optimal tier placement for a dataset using ML model
    Triggers data movement if tier change is recommended
    """
    # Get dataset
    dataset = db.query(Dataset).filter(Dataset.id == request.dataset_id).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Get ML prediction
    predicted_tier, confidence = ml_engine.predict_tier(
        access_freq_7d=dataset.access_freq_7d,
        avg_latency_ms=dataset.avg_latency_ms,
        size_bytes=dataset.size_bytes
    )
    
    current_tier = dataset.current_tier
    should_move = predicted_tier != current_tier
    movement_executed = False
    
    if should_move and request.auto_execute:
        # Create movement record FIRST to get the ID
        movement = Movement(
            dataset_id=dataset.id,
            from_tier=current_tier,
            to_tier=predicted_tier,
            size_bytes=dataset.size_bytes,
            reason="ml_prediction",
            status="pending",
            initiated_at=datetime.utcnow()
        )
        db.add(movement)
        db.commit()
        db.refresh(movement)  # Get the auto-generated ID
        
        # Trigger movement via Kafka with Movement ID
        if kafka_available and kafka_producer:
            move_event = {
                "movement_id": movement.id,  # CRITICAL: Include movement ID
                "dataset_id": dataset.id,
                "dataset_name": dataset.name,
                "from_tier": current_tier,
                "to_tier": predicted_tier,
                "size_bytes": dataset.size_bytes,
                "reason": "ml_prediction",
                "confidence": confidence,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            try:
                kafka_producer.send_move_event(move_event)
                logger.info(f"Move event sent for dataset {dataset.name}: {current_tier} -> {predicted_tier}")
                movement_executed = True  # Only true if Kafka send succeeded
            except Exception as e:
                logger.error(f"Failed to send move event: {e}")
                movement.status = "failed"
                movement.error_message = f"Kafka send failed: {str(e)}"
                db.commit()
                movement_executed = False
        else:
            logger.warning("Kafka unavailable, movement record created but not queued")
            movement.status = "failed"
            movement.error_message = "Kafka unavailable"
            db.commit()
            movement_executed = False
    
    return DecisionResponse(
        dataset_id=dataset.id,
        dataset_name=dataset.name,
        current_tier=current_tier,
        recommended_tier=predicted_tier,
        confidence=confidence,
        should_move=should_move,
        executed=movement_executed  # Only true if actually queued in Kafka
    )

@app.get("/movements", response_model=List[MovementResponse])
async def list_movements(limit: int = 50, db: Session = Depends(get_db)):
    """List recent data movements"""
    movements = db.query(Movement).order_by(Movement.initiated_at.desc()).limit(limit).all()
    
    result = []
    for mov in movements:
        dataset = db.query(Dataset).filter(Dataset.id == mov.dataset_id).first()
        result.append(MovementResponse(
            id=mov.id,
            dataset_id=mov.dataset_id,
            dataset_name=dataset.name if dataset else "Unknown",
            from_tier=mov.from_tier,
            to_tier=mov.to_tier,
            size_bytes=mov.size_bytes,
            reason=mov.reason,
            status=mov.status,
            initiated_at=mov.initiated_at,
            completed_at=mov.completed_at,
            error_message=mov.error_message
        ))
    
    return result

@app.put("/movements/{movement_id}/status")
async def update_movement_status(
    movement_id: int,
    status: str,
    error_message: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Update movement status (called by mover service)"""
    movement = db.query(Movement).filter(Movement.id == movement_id).first()
    
    if not movement:
        raise HTTPException(status_code=404, detail="Movement not found")
    
    movement.status = status
    if status == "completed":
        movement.completed_at = datetime.utcnow()
        
        # Update dataset tier
        dataset = db.query(Dataset).filter(Dataset.id == movement.dataset_id).first()
        if dataset:
            dataset.current_tier = movement.to_tier
            dataset.version += 1
    
    if error_message:
        movement.error_message = error_message
    
    db.commit()
    
    return {"message": "Status updated", "movement_id": movement_id, "status": status}

@app.post("/datasets/{dataset_id}/access")
async def record_access(
    dataset_id: int,
    latency_ms: float = 0.0,
    db: Session = Depends(get_db)
):
    """Record a dataset access event (updates metrics with 7-day windowing)"""
    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Reset window if needed
    _reset_access_window_if_needed(dataset)
    
    # Update access metrics
    dataset.access_freq_7d += 1
    dataset.last_accessed = datetime.utcnow()
    
    # Update average latency (exponential moving average)
    if dataset.avg_latency_ms == 0:
        dataset.avg_latency_ms = latency_ms
    else:
        dataset.avg_latency_ms = (dataset.avg_latency_ms * 0.9) + (latency_ms * 0.1)
    
    db.commit()
    
    # Send access event to Kafka (if available)
    if kafka_available and kafka_producer:
        access_event = {
            "dataset_id": dataset.id,
            "dataset_name": dataset.name,
            "tier": dataset.current_tier,
            "latency_ms": latency_ms,
            "timestamp": datetime.utcnow().isoformat()
        }
        try:
            kafka_producer.send_access_event(access_event)
        except Exception as e:
            logger.warning(f"Failed to send access event to Kafka: {e}")
    
    return {"message": "Access recorded", "dataset_id": dataset_id}

# Global state for metric throttling
_last_metric_snapshot = None
_last_snapshot_time = None
METRIC_SNAPSHOT_INTERVAL = timedelta(minutes=5)  # Only snapshot every 5 minutes

@app.get("/metrics", response_model=MetricsResponse)
async def get_metrics(db: Session = Depends(get_db)):
    """Get system-wide metrics and statistics with throttled persistence"""
    global _last_metric_snapshot, _last_snapshot_time
    
    # Tier distribution
    datasets = db.query(Dataset).all()
    tier_counts = {"hot": 0, "warm": 0, "cold": 0}
    tier_sizes = {"hot": 0, "warm": 0, "cold": 0}
    
    total_datasets = len(datasets)
    total_size = 0
    
    for ds in datasets:
        tier_counts[ds.current_tier] += 1
        tier_sizes[ds.current_tier] += ds.size_bytes
        total_size += ds.size_bytes
    
    tier_distribution = [
        TierDistribution(tier="hot", count=tier_counts["hot"], size_bytes=tier_sizes["hot"]),
        TierDistribution(tier="warm", count=tier_counts["warm"], size_bytes=tier_sizes["warm"]),
        TierDistribution(tier="cold", count=tier_counts["cold"], size_bytes=tier_sizes["cold"])
    ]
    
    # Cost calculation
    cost_per_gb = {"hot": 0.023, "warm": 0.015, "cold": 0.004}
    total_cost = sum(
        (tier_sizes[tier] / (1024**3)) * cost_per_gb[tier]
        for tier in ["hot", "warm", "cold"]
    )
    
    # Movement stats
    movements = db.query(Movement).all()
    total_movements = len(movements)
    successful_movements = len([m for m in movements if m.status == "completed"])
    failed_movements = len([m for m in movements if m.status == "failed"])
    
    # Throttled metric persistence - only snapshot every 5 minutes
    now = datetime.utcnow()
    should_snapshot = (
        _last_snapshot_time is None or 
        (now - _last_snapshot_time) >= METRIC_SNAPSHOT_INTERVAL
    )
    
    if should_snapshot:
        # TODO: evaluate moving this persistence into a background worker or materialized view
        metric_snapshot = Metric(
            timestamp=now,
            total_datasets=total_datasets,
            hot_count=tier_counts["hot"],
            warm_count=tier_counts["warm"],
            cold_count=tier_counts["cold"],
            hot_size_bytes=tier_sizes["hot"],
            warm_size_bytes=tier_sizes["warm"],
            cold_size_bytes=tier_sizes["cold"],
            estimated_cost_usd=round(total_cost, 2),
            avg_latency_ms=sum(ds.avg_latency_ms for ds in datasets) / max(total_datasets, 1)
        )
        db.add(metric_snapshot)
        db.commit()
        _last_snapshot_time = now
        logger.info(f"Metric snapshot persisted (next in {METRIC_SNAPSHOT_INTERVAL.total_seconds()/60:.0f} min)")
    
    return MetricsResponse(
        total_datasets=total_datasets,
        total_size_bytes=total_size,
        tier_distribution=tier_distribution,
        estimated_cost_usd=round(total_cost, 2),
        total_movements=total_movements,
        successful_movements=successful_movements,
        failed_movements=failed_movements
    )

@app.get("/metrics/history")
async def get_metrics_history(hours: int = 24, db: Session = Depends(get_db)):
    """Get historical metrics for time-series analysis"""
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)
    metrics = db.query(Metric).filter(Metric.timestamp >= cutoff_time).order_by(Metric.timestamp).all()
    
    return [{
        "timestamp": m.timestamp.isoformat(),
        "total_datasets": m.total_datasets,
        "tier_distribution": {
            "hot": {"count": m.hot_count, "size_bytes": m.hot_size_bytes},
            "warm": {"count": m.warm_count, "size_bytes": m.warm_size_bytes},
            "cold": {"count": m.cold_count, "size_bytes": m.cold_size_bytes}
        },
        "estimated_cost_usd": m.estimated_cost_usd,
        "avg_latency_ms": m.avg_latency_ms
    } for m in metrics]

@app.post("/ml/retrain")
async def retrain_model():
    """Retrain the ML model with current data"""
    try:
        global ml_available
        ml_engine.train_model()
        ml_available = True
        return {"message": "Model retrained successfully"}
    except Exception as e:
        logger.error(f"Model retraining failed: {e}")
        raise HTTPException(status_code=500, detail=f"Training failed: {str(e)}")

@app.get("/ml/model-info")
async def get_model_info():
    """Get information about the current ML model"""
    info = ml_engine.get_model_info()
    info["available"] = ml_available
    return info

@app.post("/admin/cleanup-metrics")
async def cleanup_old_metrics(retention_days: int = 30, db: Session = Depends(get_db)):
    """
    Manually trigger cleanup of old metric snapshots
    Default retention: 30 days
    """
    from migrations import _cleanup_old_metrics
    try:
        _cleanup_old_metrics(db, retention_days)
        return {"message": f"Cleanup completed, retaining last {retention_days} days"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cleanup failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
