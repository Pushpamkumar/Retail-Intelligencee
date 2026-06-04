from sqlalchemy import Column, String, Integer, Float, DateTime, BigInteger, Text, ForeignKey, JSON, Boolean
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base

class CameraModel(Base):
    __tablename__ = "cameras"

    id = Column(String(50), primary_key=True)
    name = Column(String(100), nullable=False)
    location = Column(String(100), nullable=False)
    stream_url = Column(String(255), nullable=False)
    status = Column(String(20), default="offline") # active, offline, error
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    zones = relationship("ZoneModel", back_populates="camera", cascade="all, delete-orphan")
    tracked_persons = relationship("TrackedPersonModel", back_populates="camera", cascade="all, delete-orphan")
    events = relationship("EventModel", back_populates="camera", cascade="all, delete-orphan")
    anomalies = relationship("AnomalyModel", back_populates="camera")


class ZoneModel(Base):
    __tablename__ = "zones"

    id = Column(String(50), primary_key=True)
    camera_id = Column(String(50), ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False)
    name = Column(String(100), nullable=False)
    polygon_coordinates = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationships
    camera = relationship("CameraModel", back_populates="zones")
    events = relationship("EventModel", back_populates="zone")


class TrackedPersonModel(Base):
    __tablename__ = "tracked_persons"

    id = Column(String(100), primary_key=True)
    camera_id = Column(String(50), ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False)
    person_id_seq = Column(Integer, nullable=False)
    entry_time = Column(DateTime, nullable=False, default=datetime.utcnow)
    exit_time = Column(DateTime, nullable=True)
    dwell_time_sec = Column(Float, default=0.0)

    # Relationships
    camera = relationship("CameraModel", back_populates="tracked_persons")


class EventModel(Base):
    __tablename__ = "events"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    event_type = Column(String(50), nullable=False)
    camera_id = Column(String(50), ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False)
    zone_id = Column(String(50), ForeignKey("zones.id", ondelete="SET NULL"), nullable=True)
    person_id = Column(String(100), ForeignKey("tracked_persons.id", ondelete="SET NULL"), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    event_metadata = Column("metadata", JSON, default={})

    # Relationships
    camera = relationship("CameraModel", back_populates="events")
    zone = relationship("ZoneModel", back_populates="events")


class AnomalyModel(Base):
    __tablename__ = "anomalies"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    camera_id = Column(String(50), ForeignKey("cameras.id", ondelete="SET NULL"), nullable=True)
    anomaly_type = Column(String(50), nullable=False)
    confidence_score = Column(Float, nullable=False)
    description = Column(Text, nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    status = Column(String(20), default="active") # active, resolved, ignored

    # Relationships
    camera = relationship("CameraModel", back_populates="anomalies")


class AnalyticsModel(Base):
    __tablename__ = "analytics"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    metric_type = Column(String(50), nullable=False)
    dimension = Column(String(100), nullable=False)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False, default=datetime.utcnow)
    details = Column(JSON, default={})


class POSTransactionModel(Base):
    __tablename__ = "pos_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(50), nullable=False)
    timestamp = Column(DateTime, nullable=True)
    basket_value_inr = Column(Float, nullable=True)
    coupon_code = Column(String(100), nullable=True)
    offer_name = Column(String(200), nullable=True)
    discount_code = Column(String(100), nullable=True)
    invoice_number = Column(String(100), nullable=True)
    invoice_type = Column(String(50), nullable=True)
    order_date = Column(String(50), nullable=False)
    order_time = Column(String(50), nullable=False)
    store_id = Column(String(50), nullable=False)
    store_name = Column(String(100), nullable=True)
    city = Column(String(100), nullable=True)
    customer_name = Column(String(100), nullable=True)
    customer_number = Column(String(50), nullable=True)
    sku = Column(String(100), nullable=True)
    product_id = Column(String(50), nullable=False)
    ean = Column(String(50), nullable=True)
    product_name = Column(String(255), nullable=True)
    brand_name = Column(String(100), nullable=False)
    dep_name = Column(String(100), nullable=True)
    sub_category = Column(String(100), nullable=True)
    brand_type = Column(String(50), nullable=True)
    qty = Column(Integer, default=1)
    gmv = Column(Float, default=0.0)
    nmv = Column(Float, default=0.0)
    coupon_amount = Column(Float, default=0.0)
    item_promotion = Column(Float, default=0.0)
    amt_without_gwp = Column(Float, default=0.0)
    total_amount = Column(Float, default=0.0)


# ==========================================
# Challenge Specific Table Definitions
# ==========================================
class ChallengeEventModel(Base):
    __tablename__ = "challenge_events"

    event_id = Column(String(100), primary_key=True)
    store_id = Column(String(50), nullable=False, index=True)
    camera_id = Column(String(50), nullable=False)
    visitor_id = Column(String(50), nullable=False, index=True)
    event_type = Column(String(50), nullable=False, index=True) # ENTRY, EXIT, ZONE_ENTER, etc.
    timestamp = Column(DateTime, nullable=False, index=True)
    zone_id = Column(String(50), nullable=True)
    dwell_ms = Column(BigInteger, default=0)
    is_staff = Column(Boolean, default=False)
    confidence = Column(Float, default=1.0)
    metadata_json = Column("metadata", JSON, default={})
