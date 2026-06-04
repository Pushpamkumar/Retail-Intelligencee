from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Any
import logging
from app.db import get_db
from app.models import ChallengeEventModel
from app.schemas import ChallengeEventSchema, IngestionResponse, IngestionResultError

logger = logging.getLogger("IngestionService")
router = APIRouter()

@router.post("/events/ingest", response_model=IngestionResponse)
def ingest_events(batch: List[Any], db: Session = Depends(get_db)):
    """
    Ingests batches of up to 500 store events.
    Validates, deduplicates, and stores events idempotently by event_id.
    """
    if len(batch) > 500:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch size exceeds maximum limit of 500 events."
        )

    ingested_count = 0
    failed_count = 0
    errors = []
    seen_ids = set()

    for idx, raw_event in enumerate(batch):
        # Handle dict or other validation failures
        if not isinstance(raw_event, dict):
            failed_count += 1
            errors.append(IngestionResultError(
                index=idx,
                reason="Event must be a JSON object"
            ))
            continue

        event_id = raw_event.get("event_id")

        # 1. Pydantic validation for flat StoreEvent schema
        try:
            validated_event = ChallengeEventSchema.model_validate(raw_event)
        except Exception as e:
            failed_count += 1
            # Extract error summary
            errors.append(IngestionResultError(
                index=idx,
                event_id=event_id,
                reason=str(e)
            ))
            continue

        # 2. Deduplicate in same batch
        if validated_event.event_id in seen_ids:
            # Skip duplicates within the batch to ensure idempotency
            ingested_count += 1
            continue
        seen_ids.add(validated_event.event_id)

        # 3. Deduplicate against Database (Idempotency check)
        existing = db.query(ChallengeEventModel).filter(ChallengeEventModel.event_id == validated_event.event_id).first()
        if existing:
            # Already stored, count as processed and skip
            ingested_count += 1
            continue

        # 4. Persistence
        try:
            db_event = ChallengeEventModel(
                event_id=validated_event.event_id,
                store_id=validated_event.store_id,
                camera_id=validated_event.camera_id,
                visitor_id=validated_event.visitor_id,
                event_type=validated_event.event_type,
                timestamp=validated_event.timestamp,
                zone_id=validated_event.zone_id,
                dwell_ms=validated_event.dwell_ms,
                is_staff=validated_event.is_staff,
                confidence=validated_event.confidence,
                metadata_json=validated_event.metadata.model_dump() if validated_event.metadata else {}
            )
            db.add(db_event)
            db.commit()
            ingested_count += 1
        except Exception as e:
            db.rollback()
            failed_count += 1
            errors.append(IngestionResultError(
                index=idx,
                event_id=validated_event.event_id,
                reason=f"Database write failure: {e}"
            ))

    return IngestionResponse(
        ingested=ingested_count,
        failed=failed_count,
        errors=errors
    )
