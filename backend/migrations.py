"""
Database Migration Utilities
Handles schema migrations for production deployments
"""

from sqlalchemy import text
from sqlalchemy.orm import Session
from database import engine, SessionLocal
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def run_migrations():
    """
    Run all pending database migrations
    Safe to call multiple times - checks if migration is needed
    """
    db = SessionLocal()
    
    try:
        # Migration 1: Add access_window_start column if it doesn't exist
        _add_access_window_start_column(db)
        
        # Migration 2: Add retention cleanup
        _cleanup_old_metrics(db)
        
        logger.info("All migrations completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def _add_access_window_start_column(db: Session):
    """
    Add access_window_start column to datasets table
    Handles both new installations and existing databases
    """
    try:
        # Check if column exists (SQLite-specific)
        result = db.execute(text("PRAGMA table_info(datasets)"))
        columns = [row[1] for row in result]
        
        if 'access_window_start' not in columns:
            logger.info("Adding access_window_start column to datasets table")
            
            # Add the column with a default value
            db.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN access_window_start TIMESTAMP 
                DEFAULT CURRENT_TIMESTAMP
            """))
            
            # Update existing rows to have non-null values
            db.execute(text("""
                UPDATE datasets 
                SET access_window_start = COALESCE(last_accessed, created_at, CURRENT_TIMESTAMP)
                WHERE access_window_start IS NULL
            """))
            
            db.commit()
            logger.info("access_window_start column added successfully")
        else:
            logger.info("access_window_start column already exists, skipping")
            
            # Still ensure no NULL values exist
            result = db.execute(text("""
                SELECT COUNT(*) FROM datasets WHERE access_window_start IS NULL
            """))
            null_count = result.scalar()
            
            if null_count > 0:
                logger.info("Fixing %s NULL access_window_start values", null_count)
                db.execute(text("""
                    UPDATE datasets 
                    SET access_window_start = COALESCE(last_accessed, created_at, CURRENT_TIMESTAMP)
                    WHERE access_window_start IS NULL
                """))
                db.commit()
                logger.info("NULL values fixed")
                
    except Exception as e:
        logger.warning(f"Migration note: {e}")
        # For PostgreSQL/MySQL, different approach needed
        try:
            # Try PostgreSQL/MySQL style
            db.execute(text("""
                ALTER TABLE datasets 
                ADD COLUMN IF NOT EXISTS access_window_start TIMESTAMP 
                DEFAULT CURRENT_TIMESTAMP
            """))
            db.commit()
        except:
            pass  # Column might already exist

def _cleanup_old_metrics(db: Session, retention_days: int = 30):
    """
    Clean up old metric snapshots to prevent unbounded growth
    Keeps metrics from the last N days only
    """
    try:
        result = db.execute(text("""
            DELETE FROM metrics 
            WHERE timestamp < datetime('now', '-' || :days || ' days')
        """), {"days": retention_days})
        
        deleted_count = result.rowcount
        if deleted_count > 0:
            logger.info(
                "Cleaned up %s old metric snapshots (>%s days)", deleted_count, retention_days
            )
        
        db.commit()
    except Exception as e:
        logger.warning(f"Metric cleanup note: {e}")

def get_migration_status() -> dict:
    """Get the current migration status"""
    db = SessionLocal()
    
    try:
        # Check for access_window_start column
        result = db.execute(text("PRAGMA table_info(datasets)"))
        columns = [row[1] for row in result]
        has_access_window = 'access_window_start' in columns
        
        # Check for metrics count
        result = db.execute(text("SELECT COUNT(*) FROM metrics"))
        metrics_count = result.scalar()
        
        return {
            "migrations_applied": has_access_window,
            "access_window_column": has_access_window,
            "metrics_count": metrics_count,
            "status": "ready" if has_access_window else "needs_migration"
        }
    except Exception as e:
        return {
            "migrations_applied": False,
            "error": str(e),
            "status": "error"
        }
    finally:
        db.close()

if __name__ == "__main__":
    # Can be run standalone for manual migrations
    logging.basicConfig(level=logging.INFO)
    logger.info("Running database migrations (standalone)")
    run_migrations()
    logger.info("Migrations complete")
    
    status = get_migration_status()
    logger.info("Status: %s", status)
