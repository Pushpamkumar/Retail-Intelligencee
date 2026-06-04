import os
import time
import uuid
import logging
import csv
from datetime import datetime
from fastapi import FastAPI, Request, Response, Depends
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, DBAPIError

from app.db import init_tables, get_db_context, get_db
from app.models import CameraModel, ZoneModel, POSTransactionModel, ChallengeEventModel
from app.ingestion import router as ingestion_router
from app.metrics import router as metrics_router
from app.funnel import router as funnel_router
from app.anomalies import router as anomalies_router
from app.health import router as health_router
from app.heatmap import router as heatmap_router
from app.dashboard import router as dashboard_router, telemetry_broadcast_loop

telemetry_broadcast_task = None

# Configure structured logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s")
logger = logging.getLogger("StoreIntelligenceAPI")

app = FastAPI(
    title="Purplle Store Intelligence API",
    description="Challenge compliant and dashboard-integrated REST analytics endpoints.",
    version="2026.1.0"
)

# CORS configurations
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# Structured Logging Middleware
# ==========================================
@app.middleware("http")
async def structured_logging_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-ID", str(uuid.uuid4()))
    request.state.trace_id = trace_id

    # 1. Extract store_id from request path parameter if matching /stores/{id}/...
    path = request.url.path
    store_id = None
    if "/stores/" in path:
        parts = path.split("/")
        try:
            idx = parts.index("stores")
            if idx + 1 < len(parts):
                store_id = parts[idx + 1]
        except ValueError:
            pass

    # 2. Extract event_count for ingest POST request
    event_count = 0
    if path == "/events/ingest" and request.method == "POST":
        body = await request.body()
        try:
            import json
            payload = json.loads(body)
            if isinstance(payload, list):
                event_count = len(payload)
        except Exception:
            pass
        # Reset receive channel so routers can read body stream again
        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}
        request._receive = receive

    start_time = time.time()
    try:
        response = await call_next(request)
        latency_ms = int((time.time() - start_time) * 1000)
        
        # Log statement as requested by challenge spec
        logger.info(
            f"request: trace_id={trace_id} store_id={store_id} endpoint={path} "
            f"latency_ms={latency_ms} event_count={event_count} status_code={response.status_code}"
        )
        response.headers["X-Trace-ID"] = trace_id
        return response
    except Exception as e:
        latency_ms = int((time.time() - start_time) * 1000)
        logger.error(
            f"request_failed: trace_id={trace_id} store_id={store_id} endpoint={path} "
            f"latency_ms={latency_ms} event_count={event_count} error={e}"
        )
        raise e

# ==========================================
# Database 503 Graceful Degradation Handler
# ==========================================
@app.exception_handler(OperationalError)
async def db_operational_exception_handler(request: Request, exc: OperationalError):
    logger.error(f"Database operational connection failure: {exc}")
    return JSONResponse(
        status_code=503,
        content={
            "error": "Service Unavailable",
            "detail": "The database is temporarily offline or connection timed out.",
            "code": "DATABASE_OFFLINE"
        }
    )

@app.exception_handler(DBAPIError)
async def db_api_exception_handler(request: Request, exc: DBAPIError):
    logger.error(f"Database query error: {exc}")
    return JSONResponse(
        status_code=503,
        content={
            "error": "Service Unavailable",
            "detail": "A database query failed due to connectivity constraints.",
            "code": "DATABASE_QUERY_ERROR"
        }
    )

# ==========================================
# Startup Database Seeders
# ==========================================
def seed_metadata():
    """Seeds default cameras and zones into the database on startup."""
    logger.info("Seeding metadata parameters...")
    try:
        # Predefined layout zones and cameras from spreadsheet configuration
        with get_db_context() as db:
            cams = [
                {"id": "cam_01", "name": "Store Main Entrance", "loc": "Entrance"},
                {"id": "cam_02", "name": "Cosmetics Aisle", "loc": "Aisle 3 Cosmetics"},
                {"id": "cam_03", "name": "Skincare Island", "loc": "Aisle 4 Skincare"},
                {"id": "cam_04", "name": "Billing Checkout 1", "loc": "Cash Counter 1"},
                {"id": "cam_05", "name": "Billing Checkout 2", "loc": "Cash Counter 2"},
            ]
            for c in cams:
                existing = db.query(CameraModel).filter(CameraModel.id == c["id"]).first()
                if not existing:
                    db.add(CameraModel(
                        id=c["id"],
                        name=c["name"],
                        location=c["loc"],
                        stream_url=f"mock://{c['id']}",
                        status="active"
                    ))

            zones = [
                {"id": "zone_entrance", "camera_id": "cam_01", "name": "Entrance Vestibule"},
                {"id": "zone_cosmetics", "camera_id": "cam_02", "name": "Cosmetics Section"},
                {"id": "zone_skincare", "camera_id": "cam_03", "name": "Skincare Section"},
                {"id": "zone_billing", "camera_id": "cam_04", "name": "Billing Counter Queue"},
                {"id": "zone_billing_2", "camera_id": "cam_05", "name": "Billing Counter Queue 2"}
            ]
            for z in zones:
                existing = db.query(ZoneModel).filter(ZoneModel.id == z["id"]).first()
                if not existing:
                    db.add(ZoneModel(
                        id=z["id"],
                        camera_id=z["camera_id"],
                        name=z["name"],
                        polygon_coordinates=[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0]]
                    ))
            db.commit()
    except Exception as e:
        logger.error(f"Error seeding metadata: {e}")

def seed_pos_transactions():
    """Seeds transaction logs from local CSV source files into the database."""
    logger.info("Verifying transaction database logs...")
    try:
        with get_db_context() as db:
            # Clear old transactions and re-seed to ensure correct store_id mapping and today's timestamps
            db.query(POSTransactionModel).delete()
            db.commit()

            # Read POS - sample transactionsb1e826f.csv
            csv_path = "POS - sample transactionsb1e826f.csv"
            if not os.path.exists(csv_path):
                # Check alternative files
                csv_path = "Brigade_Bangalore_10_April_26 (1)bc6219c.csv"
                
            if os.path.exists(csv_path):
                logger.info(f"Parsing transactions from source: {csv_path}")
                with open(csv_path, mode="r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Parse time order fields or use defaults
                        order_date = row.get("order_date", "10-04-2026")
                        order_time = row.get("order_time", "12:00:00")
                        
                        try:
                            # Map order date and time to datetime
                            dt_str = f"{order_date} {order_time}"
                            dt = datetime.strptime(dt_str, "%d-%m-%y %H:%M:%S")
                        except Exception:
                            try:
                                dt = datetime.strptime(dt_str, "%d-%m-%Y %H:%M:%S")
                            except Exception:
                                dt = datetime.now()

                        # Shift the date part to today so that it falls on the same day as the live events
                        today = datetime.now()
                        dt = dt.replace(year=today.year, month=today.month, day=today.day)
                        order_date = dt.strftime("%d-%m-%Y")

                        # Align store ID: map ST1008 (or others) to STORE_BLR_002
                        store_id = row.get("store_id", "STORE_BLR_002")
                        if store_id == "ST1008":
                            store_id = "STORE_BLR_002"

                        total_amount = float(row.get("total_amount", 0.0) or 0.0)
                        
                        gmv_raw = row.get("GMV", row.get("gmv"))
                        gmv_val = float(gmv_raw) if gmv_raw is not None else total_amount
                        
                        nmv_raw = row.get("NMV", row.get("nmv"))
                        nmv_val = float(nmv_raw) if nmv_raw is not None else total_amount

                        db.add(POSTransactionModel(
                            order_id=row.get("order_id", row.get("transaction_id", "TXN")),
                            store_id=store_id,
                            timestamp=dt,
                            basket_value_inr=float(row.get("total_amount", row.get("basket_value_inr", total_amount)) or total_amount),
                            coupon_code=row.get("coupon_code"),
                            offer_name=row.get("offer_name"),
                            discount_code=row.get("discount_code"),
                            invoice_number=row.get("invoice_number"),
                            invoice_type=row.get("invoice_type"),
                            order_date=order_date,
                            order_time=order_time,
                            product_id=row.get("product_id", ""),
                            brand_name=row.get("brand_name", ""),
                            total_amount=total_amount,
                            qty=int(row.get("qty", 1) or 1),
                            gmv=gmv_val,
                            nmv=nmv_val,
                            coupon_amount=float(row.get("coupon_amount", 0.0) or 0.0),
                            item_promotion=float(row.get("item_promotion", 0.0) or 0.0),
                            amt_without_gwp=float(row.get("amt_without_gwp", 0.0) or 0.0)
                        ))
                db.commit()
                logger.info("Successfully seeded updated POS transactions.")
    except Exception as e:
        logger.error(f"Error seeding transaction records: {e}")

@app.on_event("startup")
def app_startup():
    global telemetry_broadcast_task
    import asyncio
    logger.info("Booting challenge API engine...")
    init_tables()
    seed_metadata()
    seed_pos_transactions()
    telemetry_broadcast_task = asyncio.create_task(telemetry_broadcast_loop())

@app.on_event("shutdown")
def app_shutdown():
    global telemetry_broadcast_task
    if telemetry_broadcast_task:
        telemetry_broadcast_task.cancel()
        logger.info("Telemetry broadcast task stopped.")

# ==========================================
# Router Integration
# ==========================================
app.include_router(ingestion_router, tags=["Ingestion"])
app.include_router(metrics_router, tags=["Metrics"])
app.include_router(funnel_router, tags=["Funnel"])
app.include_router(anomalies_router, tags=["Anomalies"])
app.include_router(health_router, tags=["Health"])
app.include_router(heatmap_router, tags=["Heatmap"])
app.include_router(dashboard_router)

# ==========================================
# Legacy Dashboard support & Web UI
# ==========================================
@app.get("/", response_class=HTMLResponse, tags=["Dashboard UI"])
def serve_dashboard():
    """Renders the HTML5 Live Visual Dashboard at the root endpoint."""
    try:
        with open("backend/static/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    except FileNotFoundError:
        return HTMLResponse(content="<h1>Purplle AI Sight API operational</h1>")

# Mount static files folder
if os.path.exists("backend/static"):
    app.mount("/static", StaticFiles(directory="backend/static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
