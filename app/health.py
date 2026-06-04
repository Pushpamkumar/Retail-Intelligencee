from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime
from typing import Dict, Any
import logging
from app.db import get_db
from app.models import ChallengeEventModel

logger = logging.getLogger("HealthService")
router = APIRouter()

@router.get("/health")
def get_service_health(db: Session = Depends(get_db)):
    """
    Returns API service health, last event timestamp per store,
    and a STALE_FEED warning if feed lag exceeds 10 minutes.
    """
    db_status = "unhealthy"
    try:
        # DB connectivity probe
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Healthcheck Database connection failed: {e}")
        raise HTTPException(
            status_code=503,
            detail="Database connection unavailable"
        )

    # Fetch last event timestamp per store
    stores_health = {}
    service_status = "healthy"

    try:
        # Find unique stores from ingested events
        unique_stores = db.query(ChallengeEventModel.store_id).distinct().all()
        store_ids = [s[0] for s in unique_stores] if unique_stores else ["STORE_BLR_002"]

        for sid in store_ids:
            # Query latest event timestamp
            last_timestamp = db.query(func.max(ChallengeEventModel.timestamp)).filter(
                ChallengeEventModel.store_id == sid
            ).scalar()

            if last_timestamp:
                lag = datetime.now() - last_timestamp
                lag_minutes = round(lag.total_seconds() / 60.0, 1)
                
                # Check if feed is stale (> 10 minutes lag)
                warning = None
                if lag.total_seconds() > 600:
                    warning = "STALE_FEED"
                    service_status = "warning"

                stores_health[sid] = {
                    "last_event_timestamp": last_timestamp.isoformat(),
                    "lag_minutes": lag_minutes,
                    "warning": warning
                }
            else:
                stores_health[sid] = {
                    "last_event_timestamp": None,
                    "lag_minutes": None,
                    "warning": "NO_FEED"
                }

    except Exception as e:
        logger.error(f"Error fetching store feed timestamps: {e}")
        # Gracefully handle query failures

    return {
        "status": service_status,
        "database": db_status,
        "timestamp": datetime.now().isoformat(),
        "stores": stores_health
    }
