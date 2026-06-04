import os
import time
import argparse
import logging
import cv2
import threading
from typing import Dict, Any, List

import pipeline.config as cfg
from tracker import CustomerTracker
from pipeline.detector import CustomerDetector
from pipeline.zone_analyzer import ZoneAnalyzer
from emit import emit_event

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PipelineDetector")

class CCTVProcessor:
    """Manages the full frame-to-event processing pipeline for a single camera stream."""
    def __init__(self, camera_id: str, stream_url: str):
        self.camera_id = camera_id
        self.stream_url = stream_url
        self.running = False
        
        # Load configs
        self.width = cfg.CAMERAS.get(camera_id, {}).get("width", 1280)
        self.height = cfg.CAMERAS.get(camera_id, {}).get("height", 720)
        self.fps = cfg.CAMERAS.get(camera_id, {}).get("fps", 30)

        # Initalize tracking components
        self.detector = CustomerDetector(
            model_path=cfg.YOLO_MODEL_PATH,
            conf_threshold=cfg.CONFIDENCE_THRESHOLD
        )
        self.tracker = CustomerTracker(
            max_lost_frames=cfg.MAX_TRACK_AGE_FRAMES,
            min_iou=cfg.IOU_THRESHOLD
        )
        self.zone_analyzer = ZoneAnalyzer(
            camera_id=self.camera_id,
            zone_configs=cfg.ZONES.get(self.camera_id, []),
            frame_width=self.width,
            frame_height=self.height
        )
        from pipeline.heatmap import HeatmapEngine
        self.heatmap_engine = HeatmapEngine(width=self.width, height=self.height)

        # Track state variables
        self.active_person_entries = {} # person_id -> timestamp
        self.active_person_zones = {}   # person_id -> {zone_id: enter_time}

    def _post_live_status(self, active_tracks, occupancies, fps=30.0, latency_ms=10.0):
        import urllib.request
        import json
        
        tracks_data = []
        for t in active_tracks:
            tracks_data.append({
                "person_id": int(t["person_id"]),
                "bbox": [int(x) for x in t["bbox"]],
                "centroid": [int(x) for x in t["centroid"]],
                "occluded": t.get("occluded", False)
            })
            
        try:
            heatmap_data = self.heatmap_engine.update(active_tracks)
        except Exception:
            heatmap_data = {}
            
        payload = {
            "camera_id": self.camera_id,
            "camera_name": cfg.CAMERAS.get(self.camera_id, {}).get("name", self.camera_id),
            "location": cfg.CAMERAS.get(self.camera_id, {}).get("location", "Store"),
            "fps": float(fps),
            "latency_ms": float(latency_ms),
            "active_shoppers": len(active_tracks),
            "occupancy_by_zone": occupancies,
            "tracks": tracks_data,
            "heatmap": heatmap_data,
            "timestamp": time.time(),
            "status": "active"
        }
        
        try:
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                "http://localhost:8000/store/live-status",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=0.1) as response:
                pass
        except Exception:
            pass

    def process(self):
        """Processes the video stream frame-by-frame and emits flat events."""
        logger.info(f"[{self.camera_id}] Starting video stream processing: {self.stream_url}")
        
        # Check if source is integer (e.g. webcam) or mock:// or file path
        if self.stream_url.startswith("mock://"):
            # If mock, run in simulated real-time mode
            self._process_simulation()
            return

        cap = cv2.VideoCapture(self.stream_url)
        if not cap.isOpened():
            logger.error(f"[{self.camera_id}] Failed to open video source: {self.stream_url}")
            return

        self.running = True
        frame_idx = 0
        last_frame_time = time.time()
        frame_interval = 1.0 / self.fps
        
        # Track sequence count for metadata
        session_sequences = {} # visitor_id -> current ordinal count

        while self.running:
            now_time = time.time()
            if now_time - last_frame_time < frame_interval:
                time.sleep(0.001)
                continue
            last_frame_time = now_time

            ret, frame = cap.read()
            if not ret:
                # Video file ended or capture dropped
                logger.info(f"[{self.camera_id}] End of video stream or read failure.")
                break

            frame_idx += 1
            timestamp = datetime.datetime.now() # Use local timestamp for event

            # Run detection (every 3rd frame to save CPU)
            if frame_idx % 3 == 0 or frame_idx == 1:
                detections = self.detector.detect(frame)
                self.last_detections = detections
            else:
                detections = getattr(self, "last_detections", [])

            # Update tracker
            active_tracks = self.tracker.update(detections, time.time())

            # Evaluate Zone polygons
            zone_events, occupancies = self.zone_analyzer.analyze(active_tracks, time.time())

            # Post live status to API server for the dashboard
            self._post_live_status(active_tracks, occupancies)

            # Emit Events based on pipeline transitions
            # A. ENTRY
            for track in active_tracks:
                pid = track["person_id"]
                visitor_id = f"VIS_{self.camera_id}_{pid}"
                if pid not in self.active_person_entries:
                    self.active_person_entries[pid] = timestamp
                    session_sequences[visitor_id] = 1
                    
                    emit_event(
                        store_id="STORE_BLR_002",
                        camera_id=self.camera_id,
                        visitor_id=visitor_id,
                        event_type="ENTRY",
                        timestamp=timestamp,
                        confidence=0.95,
                        metadata={"session_seq": 1}
                    )

            # B. EXIT
            active_pids = {t["person_id"] for t in active_tracks}
            for pid in list(self.active_person_entries.keys()):
                if pid not in active_pids:
                    visitor_id = f"VIS_{self.camera_id}_{pid}"
                    entry_t = self.active_person_entries.pop(pid, timestamp)
                    dwell_ms = int((timestamp - entry_t).total_seconds() * 1000)
                    
                    seq = session_sequences.get(visitor_id, 0) + 1
                    session_sequences[visitor_id] = seq

                    emit_event(
                        store_id="STORE_BLR_002",
                        camera_id=self.camera_id,
                        visitor_id=visitor_id,
                        event_type="EXIT",
                        timestamp=timestamp,
                        dwell_ms=dwell_ms,
                        confidence=0.95,
                        metadata={"session_seq": seq}
                    )

            # C. ZONE EVENTS (ZONE_ENTER, ZONE_EXIT, ZONE_DWELL)
            for ze in zone_events:
                evt_type = ze["event_type"]
                pid = ze["person_id"]
                visitor_id = f"VIS_{self.camera_id}_{pid}"
                zone_id = ze["zone_id"]
                zone_name = ze["zone_name"]

                if evt_type == "zone_entry":
                    if pid not in self.active_person_zones:
                        self.active_person_zones[pid] = {}
                    self.active_person_zones[pid][zone_id] = timestamp
                    
                    seq = session_sequences.get(visitor_id, 0) + 1
                    session_sequences[visitor_id] = seq

                    # If billing queue, also emit BILLING_QUEUE_JOIN
                    if "billing" in zone_id.lower():
                        q_depth = occupancies.get(zone_id, 1)
                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=visitor_id,
                            event_type="BILLING_QUEUE_JOIN",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            confidence=0.95,
                            metadata={"queue_depth": q_depth, "session_seq": seq}
                        )
                    else:
                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=visitor_id,
                            event_type="ZONE_ENTER",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            confidence=0.95,
                            metadata={"sku_zone": zone_name, "session_seq": seq}
                        )

                elif evt_type == "zone_exit":
                    z_entries = self.active_person_zones.get(pid, {})
                    entry_t = z_entries.pop(zone_id, timestamp)
                    dwell_ms = int((timestamp - entry_t).total_seconds() * 1000)

                    seq = session_sequences.get(visitor_id, 0) + 1
                    session_sequences[visitor_id] = seq

                    emit_event(
                        store_id="STORE_BLR_002",
                        camera_id=self.camera_id,
                        visitor_id=visitor_id,
                        event_type="ZONE_EXIT",
                        timestamp=timestamp,
                        zone_id=zone_id,
                        dwell_ms=dwell_ms,
                        confidence=0.95,
                        metadata={"sku_zone": zone_name, "session_seq": seq}
                    )

                elif evt_type == "zone_dwell_active":
                    z_entries = self.active_person_zones.get(pid, {})
                    entry_t = z_entries.get(zone_id, timestamp)
                    dwell_ms = int((timestamp - entry_t).total_seconds() * 1000)

                    if dwell_ms >= 30000: # Continuously for 30+ seconds
                        seq = session_sequences.get(visitor_id, 0) + 1
                        session_sequences[visitor_id] = seq

                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=visitor_id,
                            event_type="ZONE_DWELL",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            dwell_ms=dwell_ms,
                            confidence=0.95,
                            metadata={"sku_zone": zone_name, "session_seq": seq}
                        )

        cap.release()
        logger.info(f"[{self.camera_id}] Finished processing stream.")

    def _process_simulation(self):
        """Generates realistic simulated shopper movements and real-time events in 5Hz loop."""
        import random
        import datetime
        self.running = True
        logger.info(f"[{self.camera_id}] Engaging simulated real-time event generator at 5Hz.")

        # Active shoppers list: dict of person_id -> state dict
        shoppers = {}
        next_person_id = 5000 + random.randint(100, 900)

        # Decide zone name/id based on camera
        if self.camera_id == "cam_01":
            zone_id = "zone_entrance"
            zone_name = "Entrance Vestibule"
        elif self.camera_id == "cam_02":
            zone_id = "zone_cosmetics"
            zone_name = "Cosmetics Section"
        elif self.camera_id == "cam_03":
            zone_id = "zone_skincare"
            zone_name = "Skincare Section"
        elif self.camera_id == "cam_04":
            zone_id = "zone_billing"
            zone_name = "Billing Counter Queue"
        else:
            zone_id = "zone_billing_2"
            zone_name = "Billing Counter Queue 2"

        # Determine zone bounds in pixels
        if self.camera_id == "cam_01":
            x_min, x_max = 200, 1080
            y_min, y_max = 300, 600
        elif self.camera_id == "cam_02":
            x_min, x_max = 150, 600
            y_min, y_max = 100, 550
        elif self.camera_id == "cam_03":
            x_min, x_max = 550, 1100
            y_min, y_max = 100, 550
        else:
            x_min, x_max = 300, 900
            y_min, y_max = 250, 480

        # Loop at 5Hz
        while self.running:
            now = time.time()
            timestamp = datetime.datetime.now()

            # Randomly spawn a new shopper (max 4 concurrent shoppers per camera to keep it clean)
            if len(shoppers) < 4 and random.random() < 0.05:
                pid = next_person_id
                next_person_id += 1
                visitor_id = f"VIS_{self.camera_id}_{pid}"
                
                # Start position
                x = random.randint(x_min, x_max)
                y = random.randint(y_min, y_max)
                
                shoppers[pid] = {
                    "person_id": pid,
                    "visitor_id": visitor_id,
                    "x": x,
                    "y": y,
                    "start_time": timestamp,
                    "zone_entered": False,
                    "dwell_triggered": False,
                    "last_move": now
                }
                
                # Emit ENTRY event
                emit_event(
                    store_id="STORE_BLR_002",
                    camera_id=self.camera_id,
                    visitor_id=visitor_id,
                    event_type="ENTRY",
                    timestamp=timestamp,
                    confidence=0.95,
                    metadata={"session_seq": 1}
                )

            # Move active shoppers and check zone transitions
            active_tracks = []
            occupancy_count = 0
            pids_to_remove = []

            for pid, s in list(shoppers.items()):
                # Move shopper randomly
                dx = random.randint(-15, 15)
                dy = random.randint(-15, 15)
                s["x"] = max(50, min(1230, s["x"] + dx))
                s["y"] = max(50, min(670, s["y"] + dy))

                # Check if in zone
                in_zone = (x_min <= s["x"] <= x_max) and (y_min <= s["y"] <= y_max)

                # Zone transitions
                if in_zone and not s["zone_entered"]:
                    s["zone_entered"] = True
                    s["zone_enter_time"] = timestamp
                    
                    if "billing" in zone_id.lower():
                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=s["visitor_id"],
                            event_type="BILLING_QUEUE_JOIN",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            confidence=0.95,
                            metadata={"queue_depth": occupancy_count + 1, "session_seq": 2}
                        )
                    else:
                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=s["visitor_id"],
                            event_type="ZONE_ENTER",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            confidence=0.95,
                            metadata={"sku_zone": zone_name, "session_seq": 2}
                        )

                # Check dwell thresholds
                if s["zone_entered"] and not s["dwell_triggered"]:
                    dwell_sec = (timestamp - s["zone_enter_time"]).total_seconds()
                    if dwell_sec >= 8.0:
                        s["dwell_triggered"] = True
                        emit_event(
                            store_id="STORE_BLR_002",
                            camera_id=self.camera_id,
                            visitor_id=s["visitor_id"],
                            event_type="ZONE_DWELL",
                            timestamp=timestamp,
                            zone_id=zone_id,
                            dwell_ms=int(dwell_sec * 1000),
                            confidence=0.95,
                            metadata={"sku_zone": zone_name, "session_seq": 3}
                        )

                if s["zone_entered"]:
                    occupancy_count += 1

                # Form tracks data for live status
                cx, cy = s["x"], s["y"]
                bbox = [cx - 25, cy - 60, cx + 25, cy + 60]
                active_tracks.append({
                    "person_id": pid,
                    "bbox": bbox,
                    "centroid": [cx, cy]
                })

                # Decide if shopper exits (after some random time)
                total_duration = (timestamp - s["start_time"]).total_seconds()
                if total_duration > random.randint(15, 35) or (s["x"] < 50 or s["x"] > 1230 or s["y"] < 50 or s["y"] > 670):
                    pids_to_remove.append(pid)

            # Process exits
            for pid in pids_to_remove:
                s = shoppers.pop(pid)
                exit_time = datetime.datetime.now()
                total_dwell = int((exit_time - s["start_time"]).total_seconds() * 1000)
                
                if s["zone_entered"]:
                    zone_dwell = int((exit_time - s["zone_enter_time"]).total_seconds() * 1000)
                    emit_event(
                        store_id="STORE_BLR_002",
                        camera_id=self.camera_id,
                        visitor_id=s["visitor_id"],
                        event_type="ZONE_EXIT",
                        timestamp=exit_time,
                        zone_id=zone_id,
                        dwell_ms=zone_dwell,
                        confidence=0.95,
                        metadata={"sku_zone": zone_name, "session_seq": 4}
                    )
                
                emit_event(
                    store_id="STORE_BLR_002",
                    camera_id=self.camera_id,
                    visitor_id=s["visitor_id"],
                    event_type="EXIT",
                    timestamp=exit_time,
                    dwell_ms=total_dwell,
                    confidence=0.95,
                    metadata={"session_seq": 5}
                )

            # Send live telemetry broadcast
            self._post_live_status(
                active_tracks=active_tracks,
                occupancies={zone_id: occupancy_count},
                fps=5.0,
                latency_ms=1.5
            )

            time.sleep(0.2)

import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CCTV pipeline detector on camera streams.")
    parser.add_argument("--camera", type=str, default="all", help="Camera ID (cam_01 to cam_05) or 'all'")
    args = parser.parse_args()

    # Automatic fallback handler for missing mp4 files
    def get_stream_source(env_name, default_val, mock_fallback):
        val = os.getenv(env_name)
        if val:
            return val
        if os.path.exists(default_val):
            return default_val
        logger.info(f"CCTV file '{default_val}' not found. Auto-falling back to: {mock_fallback}")
        return mock_fallback

    # Load env overriding cameras
    cameras_override = {
        "cam_01": get_stream_source("STREAM_CAM_01", "CAM 1.mp4", "mock://entrance"),
        "cam_02": get_stream_source("STREAM_CAM_02", "CAM 2.mp4", "mock://cosmetics"),
        "cam_03": get_stream_source("STREAM_CAM_03", "CAM 3.mp4", "mock://skincare"),
        "cam_04": get_stream_source("STREAM_CAM_04", "CAM 4.mp4", "mock://billing"),
        "cam_05": get_stream_source("STREAM_CAM_05", "CAM 5.mp4", "mock://billing2"),
    }

    if args.camera == "all":
        # Launch workers for all 5 cameras in parallel threads
        threads = []
        for cid, stream in cameras_override.items():
            proc = CCTVProcessor(cid, stream)
            t = threading.Thread(target=proc.process, name=f"Thread-{cid}", daemon=True)
            threads.append(t)
            t.start()
            
        logger.info("Launched all 5 camera pipeline workers. Press Ctrl+C to terminate.")
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Shutdown requested.")
    else:
        stream = cameras_override.get(args.camera)
        if stream:
            proc = CCTVProcessor(args.camera, stream)
            proc.process()
        else:
            logger.error(f"Unknown camera ID: {args.camera}")
