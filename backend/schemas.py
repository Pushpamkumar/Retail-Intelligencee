from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime

# ==========================================
# Camera & Zone Schemas
# ==========================================
class ZoneBase(BaseModel):
    id: str
    camera_id: str
    name: str
    polygon_coordinates: List[List[float]]

    class Config:
        from_attributes = True

class CameraBase(BaseModel):
    id: str
    name: str
    location: str
    stream_url: str
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# ==========================================
# Tracking & Event Schemas
# ==========================================
class TrackedPersonResponse(BaseModel):
    id: str
    camera_id: str
    person_id_seq: int
    entry_time: datetime
    exit_time: Optional[datetime] = None
    dwell_time_sec: float

    class Config:
        from_attributes = True

class EventResponse(BaseModel):
    id: int
    event_type: str
    camera_id: str
    zone_id: Optional[str] = None
    person_id: Optional[str] = None
    timestamp: datetime
    metadata: Dict[str, Any]

    class Config:
        from_attributes = True

# ==========================================
# Anomaly & Metrics Schemas
# ==========================================
class AnomalyResponse(BaseModel):
    id: int
    camera_id: Optional[str] = None
    anomaly_type: str
    confidence_score: float
    description: Optional[str] = None
    timestamp: datetime
    status: str

    class Config:
        from_attributes = True

class AnalyticsResponse(BaseModel):
    id: int
    metric_type: str
    dimension: str
    value: float
    timestamp: datetime
    details: Dict[str, Any]

    class Config:
        from_attributes = True

# ==========================================
# Dashboard Rollup Summaries
# ==========================================
class FootfallMetric(BaseModel):
    timestamp: str
    visitors: int

class ZoneMetric(BaseModel):
    zone_id: str
    zone_name: str
    average_dwell_sec: float
    total_visitors: int
    active_occupants: int

class QueueMetric(BaseModel):
    camera_id: str
    average_queue_length: float
    max_dwell_sec: float

class DashboardSummaryResponse(BaseModel):
    live_visitor_count: int
    total_daily_footfall: int
    average_dwell_time_minutes: float
    active_anomalies_count: int
    camera_health_summary: Dict[str, int] # {"active": 3, "offline": 1}
    busiest_hour: str # "14:00 - 15:00"
    conversion_rate: float # Simulated conversion KPI based on entry/skincare visits

# ==========================================
# Live CCTV Shop-Floor Telemetry
# ==========================================
class LiveTrack(BaseModel):
    person_id: int
    bbox: List[int]
    centroid: List[int]
    occluded: bool

class CameraLiveState(BaseModel):
    camera_id: str
    camera_name: str
    location: str
    fps: float
    latency_ms: float
    active_shoppers: int
    occupancy_by_zone: Dict[str, int]
    tracks: List[LiveTrack]
    heatmap: Dict[str, Any]
    timestamp: float
    status: str
