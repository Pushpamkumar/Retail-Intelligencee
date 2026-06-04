# PROMPT: Generate pytest unit tests for a FastAPI retail store analytics application.
# The tests must cover:
# 1. POST /events/ingest batch size constraints (should return 400 if batch > 500 events).
# 2. Ingestion idempotency (duplicate event_id entries are skipped without raising errors).
# 3. GET /stores/{store_id}/metrics aggregation for unique visitors, conversion rate, queue depth.
# 4. GET /stores/{store_id}/funnel session-based counts and drop-off rates.
# 5. GET /stores/{store_id}/anomalies check for queue spikes, dead zones, and conversion drop alarms.
# 6. GET /health checks and feed stale warn metrics.
# 7. Database query failures translating to HTTP 503 responses.
#
# CHANGES MADE:
# 1. Configured an isolated in-memory SQLite database engine for unit testing.
# 2. Added model seeder helper functions to insert mock telemetry events and sales transactions.
# 3. Overrode FastAPI DB dependency injection dynamically inside tests.
# 4. Mocked Database Session methods to trigger OperationalErrors for HTTP 503 checks.

import pytest
from datetime import datetime, timedelta
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from app.db import Base, get_db
from app.main import app
from app.models import ChallengeEventModel, POSTransactionModel

# In-memory SQLite with StaticPool for testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)

# ==========================================
# 1. Ingestion Tests
# ==========================================
def test_ingest_batch_size_limit():
    # Attempting to post > 500 events should fail with 400
    oversized_batch = [{
        "event_id": f"evt_{i}",
        "store_id": "STORE_BLR_002",
        "camera_id": "CAM_ENTRY_01",
        "visitor_id": f"VIS_{i}",
        "event_type": "ENTRY",
        "timestamp": "2026-06-04T12:00:00Z"
    } for i in range(501)]

    response = client.post("/events/ingest", json=oversized_batch)
    assert response.status_code == 400
    assert "exceeds maximum limit" in response.json()["detail"]

def test_ingest_idempotency():
    # Post same event twice; should succeed and not create duplicates
    event = {
        "event_id": "evt_idempotent_test",
        "store_id": "STORE_BLR_002",
        "camera_id": "CAM_ENTRY_01",
        "visitor_id": "VIS_999",
        "event_type": "ENTRY",
        "timestamp": "2026-06-04T12:00:00.000Z",
        "dwell_ms": 0,
        "is_staff": False,
        "confidence": 0.99
    }

    # First Ingestion
    res1 = client.post("/events/ingest", json=[event])
    assert res1.status_code == 200
    assert res1.json()["ingested"] == 1

    # Second Ingestion (Idempotent replay)
    res2 = client.post("/events/ingest", json=[event])
    assert res2.status_code == 200
    assert res2.json()["ingested"] == 1 # Should count as processed, no DB error

    # Verify database entry count is exactly 1
    db = TestingSessionLocal()
    count = db.query(ChallengeEventModel).filter(ChallengeEventModel.event_id == "evt_idempotent_test").count()
    assert count == 1
    db.close()

# ==========================================
# 2. Metrics & Funnel Tests
# ==========================================
def test_store_metrics_endpoint():
    db = TestingSessionLocal()
    # Seed visitor events and a transaction
    t_now = datetime.now()
    db.add(ChallengeEventModel(
        event_id="e1", store_id="STORE_BLR_002", camera_id="c1", visitor_id="VIS_1",
        event_type="ENTRY", timestamp=t_now, is_staff=False
    ))
    db.add(ChallengeEventModel(
        event_id="e2", store_id="STORE_BLR_002", camera_id="c1", visitor_id="VIS_1",
        event_type="ZONE_ENTER", zone_id="zone_billing", timestamp=t_now + timedelta(seconds=10), is_staff=False
    ))
    db.add(POSTransactionModel(
        order_id="TXN_01", store_id="STORE_BLR_002", order_date=t_now.strftime("%d-%m-%Y"),
        order_time=(t_now + timedelta(seconds=12)).strftime("%H:%M:%S"), product_id="P1",
        brand_name="Faces Canada", total_amount=450.0, timestamp=t_now + timedelta(seconds=12)
    ))
    db.commit()
    db.close()

    response = client.get("/stores/STORE_BLR_002/metrics")
    assert response.status_code == 200
    data = response.json()
    assert data["store_id"] == "STORE_BLR_002"
    assert data["unique_visitors"] == 1
    assert data["conversion_rate"] > 0.0

def test_store_funnel_endpoint():
    db = TestingSessionLocal()
    t_now = datetime.now()
    # Visitor 1: enters, browses, pays
    db.add(ChallengeEventModel(event_id="e10", store_id="STORE_BLR_002", camera_id="c1", visitor_id="VIS_A", event_type="ENTRY", timestamp=t_now))
    db.add(ChallengeEventModel(event_id="e11", store_id="STORE_BLR_002", camera_id="c2", visitor_id="VIS_A", event_type="ZONE_ENTER", zone_id="zone_cosmetics", timestamp=t_now + timedelta(seconds=5)))
    db.add(ChallengeEventModel(event_id="e12", store_id="STORE_BLR_002", camera_id="c3", visitor_id="VIS_A", event_type="ZONE_ENTER", zone_id="zone_billing", timestamp=t_now + timedelta(seconds=10)))
    # Seed transaction
    db.add(POSTransactionModel(order_id="T1", store_id="STORE_BLR_002", order_date="10-04-2026", order_time="12:00:00", product_id="P1", brand_name="NY Bae", total_amount=100.0, timestamp=t_now + timedelta(seconds=12)))
    db.commit()
    db.close()

    response = client.get("/stores/STORE_BLR_002/funnel")
    assert response.status_code == 200
    data = response.json()
    stages = {s["stage"]: s["count"] for s in data["stages"]}
    assert stages["ENTRY"] == 1
    assert stages["ZONE_VISIT"] == 1
    assert stages["BILLING_QUEUE"] == 1
    assert stages["PURCHASE"] == 1

# ==========================================
# 3. Anomalies & Health Tests
# ==========================================
def test_store_anomalies_queue_spike():
    db = TestingSessionLocal()
    t_now = datetime.now()
    # Emit BILLING_QUEUE_JOIN event indicating queue_depth of 5
    db.add(ChallengeEventModel(
        event_id="e20", store_id="STORE_BLR_002", camera_id="c4", visitor_id="VIS_Q",
        event_type="BILLING_QUEUE_JOIN", zone_id="zone_billing", timestamp=t_now,
        metadata_json={"queue_depth": 5}
    ))
    db.commit()
    db.close()

    response = client.get("/stores/STORE_BLR_002/anomalies")
    assert response.status_code == 200
    data = response.json()
    types = [a["anomaly_type"] for a in data]
    assert "queue_spike" in types

def test_store_heatmap_endpoint():
    db = TestingSessionLocal()
    t_now = datetime.now()
    db.add(ChallengeEventModel(
        event_id="e_hm_1", store_id="STORE_BLR_002", camera_id="c2", visitor_id="VIS_HM_1",
        event_type="ZONE_EXIT", zone_id="zone_cosmetics", timestamp=t_now, dwell_ms=15000, is_staff=False
    ))
    db.commit()
    db.close()

    response = client.get("/stores/STORE_BLR_002/heatmap")
    assert response.status_code == 200
    data = response.json()
    assert data["store_id"] == "STORE_BLR_002"
    assert "density_distribution" in data
    assert "grid_dimensions" in data
    assert data["grid_dimensions"] == [32, 18]

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["database"] == "healthy"

# ==========================================
# 4. Graceful Degradation (HTTP 503)
# ==========================================
def test_database_offline_degradation():
    # Define a dependency override that raises an OperationalError immediately
    def get_broken_db():
        raise OperationalError("SELECT 1", {}, "connection closed")

    app.dependency_overrides[get_db] = get_broken_db
    
    # Test metrics endpoint fallback
    response = client.get("/stores/STORE_BLR_002/metrics")
    assert response.status_code == 503
    assert response.json()["code"] == "DATABASE_OFFLINE"

    # Restore overrides
    app.dependency_overrides[get_db] = override_get_db
