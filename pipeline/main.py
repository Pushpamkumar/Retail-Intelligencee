import time
import threading
import logging
from typing import Dict, Any, List
import pipeline.config as cfg
from pipeline.video_ingest import VideoIngester
from pipeline.detector import CustomerDetector
from pipeline.tracker import CustomerTracker
from pipeline.zone_analyzer import ZoneAnalyzer
from pipeline.event_generator import EventGenerator
from pipeline.event_streamer import EventStreamer
from pipeline.heatmap import HeatmapEngine

# Setup logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] (%(threadName)s) %(message)s")
logger = logging.getLogger("CVOrchestrator")

# Global thread-safe shared register for API queries (Near-real-time memory broker)
LATEST_CAMERA_STATUS: Dict[str, Dict[str, Any]] = {}
status_lock = threading.Lock()

class CameraPipelineWorker:
    """
    Manages the complete CV processing sequence for a single CCTV camera.
    Designed to run inside an independent OS thread for horizontal scaling.
    """
    def __init__(self, camera_id: str, camera_config: Dict[str, Any]):
        self.camera_id = camera_id
        self.config = camera_config
        self.running = False
        
        # Instantiate CV blocks
        self.ingester = VideoIngester(
            camera_id=self.camera_id,
            stream_url=self.config["stream_url"],
            width=self.config["width"],
            height=self.config["height"],
            fps=self.config["fps"]
        )
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
            frame_width=self.config["width"],
            frame_height=self.config["height"]
        )
        self.event_generator = EventGenerator(camera_id=self.camera_id)
        self.event_streamer = EventStreamer(bootstrap_servers=cfg.KAFKA_BOOTSTRAP_SERVERS)
        self.heatmap_engine = HeatmapEngine(
            width=self.config["width"],
            height=self.config["height"]
        )
        
        # Performance trackers
        self.frame_count = 0
        self.fps = 0.0
        self.latency_ms = 0.0
        self.last_detections = []

    def start(self):
        self.running = True
        self.ingester.start()
        logger.info(f"Initialized processing thread for camera: {self.camera_id}")

    def stop(self):
        self.running = False
        self.ingester.stop()
        self.event_streamer.flush()
        logger.info(f"Stopped processing thread for camera: {self.camera_id}")

    def run_loop(self):
        """Infinite execution thread loop parsing camera stream frames."""
        self.start()
        
        last_fps_calc = time.time()
        fps_frame_counter = 0
        
        while self.running:
            start_time = time.time()
            
            # 1. Grab frame from non-blocking ingestion queue
            success, frame, sim_metadata = self.ingester.get_frame()
            if not success or frame is None:
                # Frame queue is temporarily empty, sleep slightly to prevent pegging the CPU
                time.sleep(0.005)
                continue
                
            fps_frame_counter += 1
            self.frame_count += 1
            timestamp = time.time()
            
            # 2. Run object detection (YOLO inference only once every 3 frames to regulate CPU load)
            if self.frame_count % 3 == 0 or sim_metadata is not None:
                detections = self.detector.detect(frame, sim_metadata=sim_metadata)
                self.last_detections = detections
            else:
                detections = self.last_detections
            detect_time = (time.time() - start_time) * 1000.0
            
            # 3. Update spatial multi-object tracking trajectories
            active_tracks = self.tracker.update(detections, timestamp)
            
            # 4. Perform Polygon Zone analysis
            zone_events, occupancies = self.zone_analyzer.analyze(active_tracks, timestamp)
            
            # 5. Aggregate heatmap coordinates
            heatmap_data = self.heatmap_engine.update(active_tracks)
            
            # 6. Evaluate business rules & trigger events
            events = self.event_generator.process_pipeline_states(
                active_tracks=active_tracks,
                zone_events=zone_events,
                occupancies=occupancies,
                timestamp=timestamp
            )
            
            # 7. Stream events to Kafka broker (or local SQLite/File fallback)
            for evt in events:
                self.event_streamer.stream(evt)
                
            # Compute operational latency
            loop_latency = (time.time() - start_time) * 1000.0
            self.latency_ms = self.latency_ms * 0.9 + loop_latency * 0.1 # Exponential rolling avg
            
            # 8. Compute active FPS
            now = time.time()
            if now - last_fps_calc >= 1.0:
                self.fps = fps_frame_counter / (now - last_fps_calc)
                fps_frame_counter = 0
                last_fps_calc = now
                if cfg.DEBUG:
                    logger.debug(f"Cam: {self.camera_id} | FPS: {self.fps:.1f} | Latency: {self.latency_ms:.1f}ms | Active tracks: {len(active_tracks)}")
            
            # 9. Update thread-safe globally shared status for REST APIs
            self._update_shared_status(active_tracks, occupancies, heatmap_data, frame)
            
            # Regulate speed to match target FPS in simulation
            elapsed = time.time() - start_time
            sleep_time = max(0, (1.0 / self.config["fps"]) - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def _update_shared_status(self, active_tracks: List[Dict[str, Any]], occupancies: Dict[str, int], heatmap_data: Dict[str, Any], frame: Any):
        """Safely commits runtime diagnostics to LATEST_CAMERA_STATUS."""
        # Convert tracks to pure API-compatible formats
        api_tracks = []
        for track in active_tracks:
            api_tracks.append({
                "person_id": track["person_id"],
                "bbox": track["bbox"],
                "centroid": track["centroid"],
                "occluded": track.get("occluded", False)
            })
            
        with status_lock:
            LATEST_CAMERA_STATUS[self.camera_id] = {
                "camera_id": self.camera_id,
                "camera_name": self.config["name"],
                "location": self.config["location"],
                "fps": round(self.fps, 1),
                "latency_ms": round(self.latency_ms, 1),
                "active_shoppers": len([t for t in active_tracks if not t.get("occluded", False)]),
                "occupancy_by_zone": occupancies,
                "tracks": api_tracks,
                "heatmap": heatmap_data,
                "timestamp": time.time(),
                "status": "active"
            }


class StoreCVSystemManager:
    """Coordinates startup and teardown of all camera pipeline worker threads."""
    def __init__(self):
        self.workers: Dict[str, CameraPipelineWorker] = {}
        self.threads: List[threading.Thread] = []
        self.running = False
        
        # Pre-seed LATEST_CAMERA_STATUS register
        for cid, config in cfg.CAMERAS.items():
            LATEST_CAMERA_STATUS[cid] = {
                "camera_id": cid,
                "camera_name": config["name"],
                "location": config["location"],
                "fps": 0.0,
                "latency_ms": 0.0,
                "active_shoppers": 0,
                "occupancy_by_zone": {},
                "tracks": [],
                "heatmap": {},
                "timestamp": time.time(),
                "status": "offline"
            }

    def start_all(self):
        """Starts all camera worker loops inside separate threads."""
        self.running = True
        for cid, config in cfg.CAMERAS.items():
            worker = CameraPipelineWorker(camera_id=cid, camera_config=config)
            self.workers[cid] = worker
            
            t = threading.Thread(
                target=worker.run_loop, 
                name=f"WorkerThread-{cid}", 
                daemon=True
            )
            self.threads.append(t)
            t.start()
            
        logger.info("All Store CCTV Intelligence processing threads booted successfully.")

    def stop_all(self):
        """Stops all running camera threads."""
        self.running = False
        for worker in self.workers.values():
            worker.stop()
        logger.info("All camera workers requested to stop. Cleaning pipeline allocations.")


# Singleton class instance for process-level imports
cv_system_manager = StoreCVSystemManager()

if __name__ == "__main__":
    # Test script to run the standalone pipeline orchestrator
    logger.info("Starting standalone Store Intelligence Pipeline. Press Ctrl+C to terminate.")
    try:
        cv_system_manager.start_all()
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        logger.info("Termination signal received.")
    finally:
        cv_system_manager.stop_all()
