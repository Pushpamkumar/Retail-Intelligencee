from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime

class EventMetadataSchema(BaseModel):
    queue_depth: Optional[int] = None
    sku_zone: Optional[str] = None
    session_seq: Optional[int] = None

class ChallengeEventSchema(BaseModel):
    event_id: str = Field(..., description="Globally unique UUID-v4 for idempotency")
    store_id: str = Field(..., description="Identifier of the retail store")
    camera_id: str = Field(..., description="Camera identifier")
    visitor_id: str = Field(..., description="Re-ID token unique per visit session")
    event_type: str = Field(..., description="Event catalogue key e.g., ENTRY, ZONE_ENTER")
    timestamp: datetime = Field(..., description="ISO-8601 UTC timestamp")
    zone_id: Optional[str] = None
    dwell_ms: Optional[int] = 0
    is_staff: bool = False
    confidence: float = 1.0
    metadata: Optional[EventMetadataSchema] = None

class IngestionResultError(BaseModel):
    index: int
    event_id: Optional[str] = None
    reason: str

class IngestionResponse(BaseModel):
    ingested: int
    failed: int
    errors: List[IngestionResultError]
