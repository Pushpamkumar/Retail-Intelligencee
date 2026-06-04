import urllib.request
import urllib.error
import json
import logging
import uuid
import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("EventEmitter")

def emit_event(
    store_id: str,
    camera_id: str,
    visitor_id: str,
    event_type: str,
    timestamp: datetime.datetime,
    zone_id: str = None,
    dwell_ms: int = 0,
    is_staff: bool = False,
    confidence: float = 1.0,
    metadata: dict = None
) -> bool:
    """
    Formats an event into the flat JSON schema required by the challenge
    and posts it to the /events/ingest API endpoint.
    """
    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ") if isinstance(timestamp, datetime.datetime) else timestamp,
        "zone_id": zone_id,
        "dwell_ms": int(dwell_ms),
        "is_staff": is_staff,
        "confidence": round(float(confidence), 2),
        "metadata": {
            "queue_depth": metadata.get("queue_depth") if metadata else None,
            "sku_zone": metadata.get("sku_zone") if metadata else None,
            "session_seq": metadata.get("session_seq") if metadata else 1
        }
    }

    url = "http://localhost:8000/events/ingest"
    payload_bytes = json.dumps([event]).encode("utf-8") # Ingest endpoint accepts a batch list
    
    req = urllib.request.Request(
        url,
        data=payload_bytes,
        headers={"Content-Type": "application/json"}
    )
    
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            res_body = json.loads(response.read().decode("utf-8"))
            if res_body.get("failed", 0) > 0:
                logger.warning(f"Event emission partial success: {res_body}")
            return True
    except urllib.error.URLError as e:
        logger.warning(f"Could not connect to API to emit event: {e}. Event printed to console.")
        print(json.dumps(event))
        return False
    except Exception as e:
        logger.error(f"Error emitting event: {e}")
        return False

if __name__ == "__main__":
    # Test emission
    emit_event(
        store_id="STORE_BLR_002",
        camera_id="CAM_ENTRY_01",
        visitor_id="VIS_test",
        event_type="ENTRY",
        timestamp=datetime.datetime.utcnow(),
        confidence=0.98
    )
