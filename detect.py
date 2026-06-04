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
        """Generates simulated events in real time if running mock feed."""
        import random
        self.running = True
        logger.info(f"[{self.camera_id}] Engaging simulated real-time event generator.")
        
        # Send a few entries and dwells immediately to populate database
        for _ in range(5):
            if not self.running:
                break
            visitor_id = f"VIS_{self.camera_id}_{random.randint(100, 999)}"
            t = datetime.datetime.now()
            
            # Emit ENTRY
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "ENTRY", t)
            time.sleep(0.5)

            # Emit ZONE_ENTER
            zone = "COSMETICS" if self.camera_id == "cam_02" else ("SKINCARE" if self.camera_id == "cam_03" else "BILLING")
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "ZONE_ENTER", t + datetime.timedelta(seconds=2), zone_id=zone)
            time.sleep(0.5)

            # Emit ZONE_EXIT / EXIT
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "ZONE_EXIT", t + datetime.timedelta(seconds=12), zone_id=zone, dwell_ms=10000)
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "EXIT", t + datetime.timedelta(seconds=15), dwell_ms=15000)

        # Loop generating periodic traffic
        while self.running:
            time.sleep(random.randint(10, 20))
            visitor_id = f"VIS_{self.camera_id}_{random.randint(100, 999)}"
            t = datetime.datetime.now()
            
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "ENTRY", t)
            time.sleep(2)
            zone = "COSMETICS" if self.camera_id == "cam_02" else ("SKINCARE" if self.camera_id == "cam_03" else "BILLING")
            emit_event("STORE_BLR_002", self.camera_id, visitor_id, "ZONE_ENTER", t + datetime.timedelta(seconds=2), zone_id=zone)

import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CCTV pipeline detector on camera streams.")
    parser.add_argument("--camera", type=str, default="all", help="Camera ID (cam_01 to cam_05) or 'all'")
    args = parser.parse_args()

    # Load env overriding cameras
    cameras_override = {
        "cam_01": os.getenv("STREAM_CAM_01", "CAM 1.mp4"),
        "cam_02": os.getenv("STREAM_CAM_02", "CAM 2.mp4"),
        "cam_03": os.getenv("STREAM_CAM_03", "CAM 3.mp4"),
        "cam_04": os.getenv("STREAM_CAM_04", "CAM 4.mp4"),
        "cam_05": os.getenv("STREAM_CAM_05", "CAM 5.mp4"),
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
