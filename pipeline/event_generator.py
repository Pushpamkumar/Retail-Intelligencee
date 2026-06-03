import time
import uuid
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field
import pipeline.config as cfg

# ==========================================
# Pydantic Schemas for Standardized Events
# ==========================================

class EventHeader(BaseModel):
    event_id: str = Field(default_factory=lambda: f"evt_{uuid.uuid4()}")
    event_type: str
    version: str = "1.0"
    timestamp: str = Field(default_factory=lambda: time.strftime("%Y-%m-%dT%H:%M:%S"))
    camera_id: str

class CustomerEnteredPayload(BaseModel):
    person_id: int
    entry_bbox: List[int]

class CustomerExitedPayload(BaseModel):
    person_id: int
    dwell_time_sec: float

class ShelfVisitPayload(BaseModel):
    person_id: int
    zone_id: str
    zone_name: str
    dwell_time_sec: float

class QueueDetectedPayload(BaseModel):
    queue_length: int
    max_dwell_time_sec: float
    camera_id: str

class CrowdingDetectedPayload(BaseModel):
    zone_id: str
    zone_name: str
    person_count: int

class LongDwellTimePayload(BaseModel):
    person_id: int
    zone_id: str
    zone_name: str
    dwell_time_sec: float

class ZoneCongestionPayload(BaseModel):
    zone_id: str
    zone_name: str
    person_count: int
    congestion_ratio: float

class SystemAnomalyPayload(BaseModel):
    anomaly_type: str # camera_offline, camera_obstructed
    confidence: float
    description: str

class StoreTrafficPayload(BaseModel):
    traffic_status: str # low_traffic, high_traffic
    active_visitor_count: int

# Main Wrapper Event
class StoreEvent(BaseModel):
    header: EventHeader
    payload: Dict[str, Any]


class EventGenerator:
    """
    EventGenerator processes CV pipeline states and transforms them 
    into validated, structured StoreEvents based on business logic rules.
    """
    def __init__(self, camera_id: str):
        self.camera_id = camera_id
        # State to track active customer entering/exiting metrics
        self.active_person_entries = {} # person_id -> timestamp
        self.active_person_zones = {}   # person_id -> {zone_id: enter_time}
        # Keep track of events we've already triggered to prevent flooding (de-duplication)
        self.triggered_events_cache = {} # event_key -> last_trigger_timestamp

    def _should_trigger(self, event_key: str, cooldown_sec: float = 30.0) -> bool:
        """Throttling mechanism to prevent duplicate alarms (e.g. crowding) flooding Kafka."""
        now = time.time()
        last_triggered = self.triggered_events_cache.get(event_key, 0.0)
        if now - last_triggered > cooldown_sec:
            self.triggered_events_cache[event_key] = now
            return True
        return False

    def process_pipeline_states(
        self, 
        active_tracks: List[Dict[str, Any]], 
        zone_events: List[Dict[str, Any]], 
        occupancies: Dict[str, int],
        timestamp: float
    ) -> List[Dict[str, Any]]:
        """
        Main entry point for evaluating rules and compiling event structures.
        """
        output_events = []
        datetime_str = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(timestamp))

        # 1. Evaluate Customer Entry events
        for track in active_tracks:
            pid = track["person_id"]
            if pid not in self.active_person_entries:
                self.active_person_entries[pid] = timestamp
                
                # Emit customer_entered event
                hdr = EventHeader(event_type="customer_entered", timestamp=datetime_str, camera_id=self.camera_id)
                pay = CustomerEnteredPayload(person_id=pid, entry_bbox=track["bbox"])
                evt = StoreEvent(header=hdr, payload=pay.model_dump())
                output_events.append(evt.model_dump())

        # 2. Evaluate Customer Exit events
        active_pids = {t["person_id"] for t in active_tracks}
        for pid in list(self.active_person_entries.keys()):
            if pid not in active_pids:
                entry_time = self.active_person_entries.pop(pid, timestamp)
                total_duration = timestamp - entry_time
                
                hdr = EventHeader(event_type="customer_exited", timestamp=datetime_str, camera_id=self.camera_id)
                pay = CustomerExitedPayload(person_id=pid, dwell_time_sec=round(total_duration, 2))
                evt = StoreEvent(header=hdr, payload=pay.model_dump())
                output_events.append(evt.model_dump())

        # 3. Process Zone specific events (Entries, Exits, Dwell, Crowding)
        for ze in zone_events:
            event_type = ze["event_type"]
            pid = ze["person_id"]
            zone_id = ze["zone_id"]
            zone_name = ze["zone_name"]

            # Zone Entry registered
            if event_type == "zone_entry":
                if pid not in self.active_person_zones:
                    self.active_person_zones[pid] = {}
                self.active_person_zones[pid][zone_id] = ze["timestamp"]

            # Zone Exit registered -> Shelf Visit evaluation
            elif event_type == "zone_exit":
                z_entries = self.active_person_zones.get(pid, {})
                entry_t = z_entries.pop(zone_id, ze["timestamp"])
                dwell = ze["dwell_time_sec"]

                # If shopper browsed cosmetics/skincare shelves > 5 seconds, register shelf_visit
                if zone_id in ["zone_cosmetics", "zone_skincare"] and dwell >= 5.0:
                    hdr = EventHeader(event_type="shelf_visit", timestamp=datetime_str, camera_id=self.camera_id)
                    pay = ShelfVisitPayload(person_id=pid, zone_id=zone_id, zone_name=zone_name, dwell_time_sec=dwell)
                    evt = StoreEvent(header=hdr, payload=pay.model_dump())
                    output_events.append(evt.model_dump())

            # Dwell checks
            elif event_type == "zone_dwell_active":
                dwell = ze["dwell_time_sec"]
                if dwell >= cfg.DWELL_TIME_THRESHOLD_SEC:
                    event_key = f"long_dwell_{pid}_{zone_id}"
                    # Allow trigger every 60s
                    if self._should_trigger(event_key, cooldown_sec=60.0):
                        hdr = EventHeader(event_type="long_dwell_time", timestamp=datetime_str, camera_id=self.camera_id)
                        pay = LongDwellTimePayload(person_id=pid, zone_id=zone_id, zone_name=zone_name, dwell_time_sec=dwell)
                        evt = StoreEvent(header=hdr, payload=pay.model_dump())
                        output_events.append(evt.model_dump())

        # 4. Assess Zone Occupancy KPIs (Crowding, Queues, Congestion)
        for zone_id, count in occupancies.items():
            zone_name = next(
                (z["name"] for z in cfg.ZONES.get(self.camera_id, []) if z["id"] == zone_id), 
                zone_id
            )

            # Crowding Alert
            if count >= cfg.CROWDING_THRESHOLD:
                event_key = f"crowding_{zone_id}"
                if self._should_trigger(event_key, cooldown_sec=30.0):
                    hdr = EventHeader(event_type="crowding_detected", timestamp=datetime_str, camera_id=self.camera_id)
                    pay = CrowdingDetectedPayload(zone_id=zone_id, zone_name=zone_name, person_count=count)
                    evt = StoreEvent(header=hdr, payload=pay.model_dump())
                    output_events.append(evt.model_dump())
                    
                    # Also trigger zone_congestion event
                    hdr_c = EventHeader(event_type="zone_congestion", timestamp=datetime_str, camera_id=self.camera_id)
                    pay_c = ZoneCongestionPayload(zone_id=zone_id, zone_name=zone_name, person_count=count, congestion_ratio=round(count/cfg.CROWDING_THRESHOLD, 2))
                    evt_c = StoreEvent(header=hdr_c, payload=pay_c.model_dump())
                    output_events.append(evt_c.model_dump())

            # Register Queue Length alarm
            if zone_id == "zone_billing" and count > 0:
                event_key = f"queue_active_{self.camera_id}"
                # Emit queue status updates periodically (every 10s)
                if self._should_trigger(event_key, cooldown_sec=10.0):
                    # Compute max queue dwell time currently in register
                    z_entries = [self.active_person_zones.get(p, {}).get(zone_id, timestamp) for p in self.active_person_entries.keys() if p in active_pids]
                    max_dwell = timestamp - min(z_entries) if z_entries else 0.0
                    
                    hdr = EventHeader(event_type="queue_detected", timestamp=datetime_str, camera_id=self.camera_id)
                    pay = QueueDetectedPayload(queue_length=count, max_dwell_time_sec=round(max_dwell, 2), camera_id=self.camera_id)
                    evt = StoreEvent(header=hdr, payload=pay.model_dump())
                    output_events.append(evt.model_dump())

        # 5. Evaluate Traffic Statistics
        active_shop_traffic = len(active_pids)
        if active_shop_traffic >= 6:
            event_key = "high_store_traffic"
            if self._should_trigger(event_key, cooldown_sec=60.0):
                hdr = EventHeader(event_type="high_store_traffic", timestamp=datetime_str, camera_id=self.camera_id)
                pay = StoreTrafficPayload(traffic_status="high_traffic", active_visitor_count=active_shop_traffic)
                evt = StoreEvent(header=hdr, payload=pay.model_dump())
                output_events.append(evt.model_dump())
        elif active_shop_traffic == 1:
            event_key = "low_store_traffic"
            if self._should_trigger(event_key, cooldown_sec=60.0):
                hdr = EventHeader(event_type="low_store_traffic", timestamp=datetime_str, camera_id=self.camera_id)
                pay = StoreTrafficPayload(traffic_status="low_traffic", active_visitor_count=active_shop_traffic)
                evt = StoreEvent(header=hdr, payload=pay.model_dump())
                output_events.append(evt.model_dump())

        return output_events
