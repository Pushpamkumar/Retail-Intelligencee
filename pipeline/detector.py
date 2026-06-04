import time
import logging
from typing import List, Dict, Any, Tuple, Optional
import numpy as np

# Configure Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Detector")

class CustomerDetector:
    """
    CustomerDetector performs person detection on incoming camera frames.
    Wraps YOLOv8 from Ultralytics. Falls back gracefully to simulation labels 
    if PyTorch/Ultralytics is unavailable or during mock runs.
    """
    def __init__(self, model_path: str = "yolov8n.pt", conf_threshold: float = 0.4):
        self.model_path = model_path
        self.conf_threshold = conf_threshold
        self.yolo_model = None
        self.initialized = False

    def _init_yolo(self):
        """Attempts to load the ultralytics YOLO model, logging any warnings."""
        try:
            from ultralytics import YOLO
            logger.info(f"Loading YOLO model: {self.model_path}...")
            self.yolo_model = YOLO(self.model_path)
            logger.info("YOLO model loaded successfully.")
        except ImportError:
            logger.warning("Ultralytics library not installed. Detection will run in Simulation / Fallback Mode.")
        except Exception as e:
            logger.warning(f"Error loading YOLO model: {e}. Falling back to simulation mode.")

    def detect(self, frame: np.ndarray, sim_metadata: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Runs human detection on a frame.
        Args:
            frame: cv2 BGR frame matrix.
            sim_metadata: Simulated metadata from the synthetic stream, if available.
        Returns:
            List of detections: [{"bbox": [x1, y1, x2, y2], "confidence": 0.95, "class": "person"}]
        """
        start_time = time.time()
        
        # 0. Lazy initialize YOLO on first detection call (runs in background worker thread)
        if not self.initialized:
            self._init_yolo()
            self.initialized = True
            
        # 1. Fallback to high-fidelity simulation metadata if YOLO is not initialized or stream is mock
        if self.yolo_model is None or sim_metadata is not None:
            detections = []
            if sim_metadata and "detections" in sim_metadata:
                # Add tiny random noise to mock bbox to simulate real-world noise
                for det in sim_metadata["detections"]:
                    bbox = det["bbox"]
                    # Add subtle jitter to test tracker association robustness
                    import random
                    jitter_x = random.randint(-2, 2)
                    jitter_y = random.randint(-2, 2)
                    noisy_bbox = [
                        max(0, bbox[0] + jitter_x),
                        max(0, bbox[1] + jitter_y),
                        min(frame.shape[1], bbox[2] + jitter_x),
                        min(frame.shape[0], bbox[3] + jitter_y)
                    ]
                    detections.append({
                        "bbox": noisy_bbox,
                        "confidence": det["confidence"],
                        "class": "person",
                        "sim_id": det.get("sim_id") # Anchor tracker matching
                    })
            
            latency = (time.time() - start_time) * 1000.0
            # Track inference metrics
            self.last_latency_ms = latency
            return detections

        # 2. Run actual YOLO detection
        try:
            results = self.yolo_model(frame, verbose=False)[0]
            detections = []
            
            for box in results.boxes:
                cls_id = int(box.cls[0])
                conf = float(box.conf[0])
                
                # Class 0 in COCO dataset is 'person'
                if cls_id == 0 and conf >= self.conf_threshold:
                    # Bbox in xyxy coordinates
                    x1, y1, x2, y2 = box.xyxy[0].tolist()
                    detections.append({
                        "bbox": [int(x1), int(y1), int(x2), int(y2)],
                        "confidence": round(conf, 2),
                        "class": "person"
                    })
                    
            self.last_latency_ms = (time.time() - start_time) * 1000.0
            return detections
            
        except Exception as e:
            logger.error(f"YOLO Inference failure: {e}. Returning empty detections list.")
            self.last_latency_ms = (time.time() - start_time) * 1000.0
            return []
            
    def get_performance_stats(self) -> Dict[str, float]:
        """Returns performance metrics for observability."""
        return {
            "inference_latency_ms": getattr(self, "last_latency_ms", 0.0),
            "fps": 1000.0 / getattr(self, "last_latency_ms", 33.3) if getattr(self, "last_latency_ms", 0.0) > 0 else 30.0
        }
