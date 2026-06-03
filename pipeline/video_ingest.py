import cv2
import time
import threading
import queue
import numpy as np
import random
import logging
from typing import Dict, Any, Tuple, Optional
import pipeline.config as cfg

logger = logging.getLogger("VideoIngester")

class VideoIngester:
    """
    VideoIngester handles CCTV video stream ingestion.
    Supports RTSP streams, local files, and high-fidelity mock stream simulation.
    Uses multi-threading to handle frame rates and queue buffering.
    """
    def __init__(self, camera_id: str, stream_url: str, width: int = 1280, height: int = 720, fps: int = 30):
        self.camera_id = camera_id
        self.stream_url = stream_url
        self.width = width
        self.height = height
        self.fps = fps
        
        self.frame_queue = queue.Queue(maxsize=100)
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.cap: Optional[cv2.VideoCapture] = None
        self.is_mock = stream_url.startswith("mock://")
        
        # Simulation States (Used if is_mock is true)
        self.mock_customers = [] # List of dicts tracking simulated positions
        self.mock_frame_count = 0
        self._init_simulation()

    def _init_simulation(self):
        """Sets up the initial mock objects for synthetic stream generation."""
        random.seed(hash(self.camera_id))
        if self.camera_id == "cam_01": # Entrance
            # Entrance customers enter from top/sides and exit or head in
            for _ in range(3):
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [random.uniform(0.1, 0.9), random.uniform(0.4, 0.8)],
                    "velocity": [random.uniform(-0.01, 0.01), random.uniform(0.005, 0.015)],
                    "size": [50, 110]
                })
        elif self.camera_id == "cam_02": # Cosmetics
            # Dwellers browsing cosmetics counters
            for _ in range(4):
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [random.uniform(0.15, 0.45), random.uniform(0.2, 0.7)],
                    "velocity": [random.uniform(-0.002, 0.002), random.uniform(-0.002, 0.002)],
                    "size": [50, 110],
                    "state": "browsing"
                })
        elif self.camera_id == "cam_03": # Skincare
            for _ in range(3):
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [random.uniform(0.45, 0.85), random.uniform(0.2, 0.7)],
                    "velocity": [random.uniform(-0.003, 0.003), random.uniform(-0.003, 0.003)],
                    "size": [50, 110]
                })
        elif self.camera_id == "cam_04": # Billing Counter Queue
            # Customers standing in a queue lane
            for i in range(3):
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [0.3 + i * 0.12, 0.5 + random.uniform(-0.02, 0.02)],
                    "velocity": [0.0, 0.0],
                    "size": [50, 110],
                    "queue_idx": i
                })
        elif self.camera_id == "cam_05": # Billing Counter Queue 2
            # Customers standing in queue lane 2
            for i in range(2):
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [0.3 + i * 0.12, 0.6 + random.uniform(-0.02, 0.02)],
                    "velocity": [0.0, 0.0],
                    "size": [50, 110],
                    "queue_idx": i
                })

    def start(self):
        """Starts the frame reading thread."""
        self.running = True
        if self.is_mock:
            self.thread = threading.Thread(target=self._mock_producer, daemon=True)
        else:
            self.thread = threading.Thread(target=self._rtsp_producer, daemon=True)
        self.thread.start()

    def stop(self):
        """Stops the frame reading thread and releases assets."""
        self.running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        if self.cap:
            self.cap.release()

    def get_frame(self) -> Tuple[bool, Optional[np.ndarray], Optional[Dict[str, Any]]]:
        """
        Pops a frame from the ingested queue.
        Returns:
            (success, frame_data, mock_ground_truth)
        """
        try:
            # Non-blocking pop to avoid freezing the model thread
            return self.frame_queue.get_nowait()
        except queue.Empty:
            return False, None, None

    def _rtsp_producer(self):
        """Worker thread for live camera streams using OpenCV."""
        try:
            # Cast integer string (e.g. "0" for webcam) to integer index
            source = int(self.stream_url) if str(self.stream_url).isdigit() else self.stream_url
        except ValueError:
            source = self.stream_url
        self.cap = cv2.VideoCapture(source)
        if not self.cap.isOpened() and isinstance(source, str):
            logger.info(f"VideoCapture failed to open with default backend. Retrying with FFMPEG backend for: {source}")
            self.cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
            
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        
        last_frame_time = time.time()
        frame_interval = 1.0 / self.fps
        
        while self.running:
            now = time.time()
            if now - last_frame_time < frame_interval:
                time.sleep(0.001)
                continue
                
            ret, frame = self.cap.read()
            if not ret:
                # Log frame drop / Reconnect logic in production
                time.sleep(0.5)
                # Auto-reconnect stream
                self.cap.release()
                self.cap = cv2.VideoCapture(self.stream_url)
                if not self.cap.isOpened() and isinstance(self.stream_url, str):
                    self.cap = cv2.VideoCapture(self.stream_url, cv2.CAP_FFMPEG)
                continue
                
            last_frame_time = now
            
            # Keep queue size under control: drop oldest frames if queue is backing up
            if self.frame_queue.full():
                try:
                    self.frame_queue.get_nowait()
                except queue.Empty:
                    pass
                    
            # Frame payload
            self.frame_queue.put((True, frame, None))

    def _mock_producer(self):
        """Generates synthetic high-fidelity retail video frames."""
        frame_interval = 1.0 / self.fps
        last_frame_time = time.time()
        
        # Color Palettes (Sleek Dark Mode layout)
        bg_color = (20, 24, 33) # Sleek Dark Blue-Gray #141821
        wall_color = (35, 41, 54) # Accent #232936
        counter_color = (60, 50, 110) # Elegant Purple #3c326e
        text_color = (255, 255, 255)
        
        while self.running:
            now = time.time()
            if now - last_frame_time < frame_interval:
                time.sleep(0.001)
                continue
                
            last_frame_time = now
            self.mock_frame_count += 1
            
            # 1. Create base canvas frame (Dark theme)
            frame = np.zeros((self.height, self.width, 3), dtype=np.uint8)
            frame[:] = bg_color
            
            # 2. Draw static camera environment
            self._draw_environment(frame, wall_color, counter_color, text_color)
            
            # 3. Update customer simulation states
            self._update_simulation()
            
            # 4. Draw customers & build mock detection logs
            detections = []
            for cust in self.mock_customers:
                px = int(cust["pos"][0] * self.width)
                py = int(cust["pos"][1] * self.height)
                w, h = cust["size"]
                
                # Bounding Box calculations
                x1 = int(px - w/2)
                y1 = int(py - h/2)
                x2 = int(px + w/2)
                y2 = int(py + h/2)
                
                # Clip box to frame boundaries
                x1, y1 = max(0, x1), max(0, y1)
                x2, y2 = min(self.width, x2), min(self.height, y2)
                
                # Draw high-fidelity simulated customer (with sleek glowing indicators)
                # Outer glow ring
                cv2.circle(frame, (px, py), 22, (179, 90, 255), 1) # Glowing magenta ring
                # Core
                cv2.circle(frame, (px, py), 12, (255, 60, 110), -1) # Coral Pink Core
                
                # Label ID (Optional Debug helper)
                cv2.putText(frame, f"Sim ID: {cust['id']}", (x1, y1 - 10),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1, cv2.LINE_AA)
                
                # Add to detections dictionary for inference pipeline verification
                detections.append({
                    "class": "person",
                    "confidence": round(random.uniform(0.88, 0.98), 2),
                    "bbox": [x1, y1, x2, y2],
                    "sim_id": cust["id"] # Kept for tracker cross-validation
                })
            
            # Prepare metadata package
            metadata = {
                "camera_id": self.camera_id,
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "detections": detections
            }
            
            # Handle Queue Overflow
            if self.frame_queue.full():
                try:
                    self.frame_queue.get_nowait()
                except queue.Empty:
                    pass
                    
            self.frame_queue.put((True, frame, metadata))

    def _draw_environment(self, frame: np.ndarray, wall_color: Tuple[int,int,int], counter_color: Tuple[int,int,int], text_color: Tuple[int,int,int]):
        """Draws virtual walls, floor designs, and sections on the frame canvas."""
        # Draw camera watermark overlay
        cv2.putText(frame, f"LIVE FEED: {self.camera_id.upper()} | {time.strftime('%Y-%m-%d %H:%M:%S')}", 
                    (20, 35), cv2.FONT_HERSHEY_SIMPLEX, 0.6, (140, 140, 200), 1, cv2.LINE_AA)
        
        # Draw boundary grid line designs
        for x in range(0, self.width, 80):
            cv2.line(frame, (x, 0), (x, self.height), (28, 33, 45), 1)
        for y in range(0, self.height, 80):
            cv2.line(frame, (0, y), (self.width, y), (28, 33, 45), 1)

        # Draw specific layout markers based on camera
        if self.camera_id == "cam_01": # Entrance
            # Draw entrance doors and gate
            cv2.rectangle(frame, (0, int(self.height * 0.4)), (50, int(self.height * 0.9)), wall_color, -1)
            cv2.rectangle(frame, (self.width - 50, int(self.height * 0.4)), (self.width, int(self.height * 0.9)), wall_color, -1)
            cv2.putText(frame, "STORE ENTRANCE", (20, int(self.height * 0.38)), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 230, 100), 1, cv2.LINE_AA)
            
        elif self.camera_id == "cam_02": # Cosmetics
            # Cosmetics counters
            cv2.rectangle(frame, (100, 100), (250, 450), counter_color, -1)
            cv2.rectangle(frame, (100, 100), (250, 450), (179, 90, 255), 2) # Magenta glowing border
            cv2.putText(frame, "PURPLLE COSMETICS COUNTER", (95, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (179, 90, 255), 1, cv2.LINE_AA)
            
        elif self.camera_id == "cam_03": # Skincare
            # Skincare islands
            cv2.rectangle(frame, (800, 100), (1050, 450), counter_color, -1)
            cv2.rectangle(frame, (800, 100), (1050, 450), (60, 220, 255), 2) # Blue glowing border
            cv2.putText(frame, "SKINCARE ISLAND A-B", (800, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (60, 220, 255), 1, cv2.LINE_AA)
            
        elif self.camera_id == "cam_04": # Billing Checkout Counter
            # Registers
            cv2.rectangle(frame, (200, 100), (700, 180), counter_color, -1)
            cv2.rectangle(frame, (200, 100), (700, 180), (255, 100, 50), 2) # Orange glowing border
            cv2.putText(frame, "POS REGISTERS 1 - 3", (200, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 100, 50), 1, cv2.LINE_AA)
            # Draw queue guidelines
            cv2.line(frame, (250, 280), (650, 280), (100, 100, 120), 1, cv2.LINE_4)
            cv2.putText(frame, "QUEUE LANE", (250, 270), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (140, 140, 150), 1, cv2.LINE_AA)
        elif self.camera_id == "cam_05": # Billing Checkout Counter 2
            # Registers 4-6
            cv2.rectangle(frame, (200, 100), (700, 180), counter_color, -1)
            cv2.rectangle(frame, (200, 100), (700, 180), (255, 100, 50), 2)
            cv2.putText(frame, "POS REGISTERS 4 - 6", (200, 85), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 100, 50), 1, cv2.LINE_AA)
            cv2.line(frame, (250, 280), (650, 280), (100, 100, 120), 1, cv2.LINE_4)
            cv2.putText(frame, "QUEUE LANE 2", (250, 270), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (140, 140, 150), 1, cv2.LINE_AA)

        # Draw Active Configured Zone Outlines
        camera_zones = cfg.ZONES.get(self.camera_id, [])
        for z in camera_zones:
            pts = []
            for pt in z["polygon"]:
                pts.append([int(pt[0] * self.width), int(pt[1] * self.height)])
            pts = np.array(pts, np.int32).reshape((-1, 1, 2))
            # Draw translucent boundary
            cv2.polylines(frame, [pts], True, (90, 90, 150), 1)

    def _update_simulation(self):
        """Simulates customer movement cycles."""
        # 1. Entrance Simulation
        if self.camera_id == "cam_01":
            # Move customer nodes
            for cust in self.mock_customers:
                cust["pos"][0] += cust["velocity"][0]
                cust["pos"][1] += cust["velocity"][1]
                
                # Check exit bounds -> reset shopper
                if cust["pos"][1] > 0.95 or cust["pos"][0] < 0.02 or cust["pos"][0] > 0.98:
                    cust["pos"] = [random.uniform(0.3, 0.7), 0.35]
                    cust["velocity"] = [random.uniform(-0.008, 0.008), random.uniform(0.005, 0.015)]
                    cust["id"] = random.randint(100, 999)
            
            # Spawn random crowd waves occasionally
            if len(self.mock_customers) < 6 and random.random() < 0.02:
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [random.uniform(0.4, 0.6), 0.35],
                    "velocity": [random.uniform(-0.005, 0.005), random.uniform(0.005, 0.012)],
                    "size": [50, 110]
                })

        # 2. Cosmetics Counter Simulation
        elif self.camera_id == "cam_02":
            for cust in self.mock_customers:
                # Add tiny random jitter representing shopper moving objects/browsing
                cust["pos"][0] += cust["velocity"][0] + random.uniform(-0.002, 0.002)
                cust["pos"][1] += cust["velocity"][1] + random.uniform(-0.002, 0.002)
                
                # Keep within bounds
                cust["pos"][0] = max(0.08, min(0.92, cust["pos"][0]))
                cust["pos"][1] = max(0.12, min(0.88, cust["pos"][1]))
                
                # Shopper changes direction or leaves
                if random.random() < 0.01:
                    cust["velocity"] = [random.uniform(-0.004, 0.004), random.uniform(-0.004, 0.004)]
                
                # Customer leaves shop floor -> respawn
                if random.random() < 0.003:
                    cust["id"] = random.randint(100, 999)
                    cust["pos"] = [random.uniform(0.7, 0.9), random.uniform(0.6, 0.9)]
                    cust["velocity"] = [-0.005, -0.005]

        # 3. Skincare Simulation
        elif self.camera_id == "cam_03":
            for cust in self.mock_customers:
                cust["pos"][0] += cust["velocity"][0] + random.uniform(-0.003, 0.003)
                cust["pos"][1] += cust["velocity"][1] + random.uniform(-0.003, 0.003)
                
                cust["pos"][0] = max(0.08, min(0.92, cust["pos"][0]))
                cust["pos"][1] = max(0.12, min(0.88, cust["pos"][1]))
                
                if random.random() < 0.015:
                    cust["velocity"] = [random.uniform(-0.005, 0.005), random.uniform(-0.005, 0.005)]
                
                # Random shopper departure
                if random.random() < 0.002:
                    cust["id"] = random.randint(100, 999)
                    cust["pos"] = [random.uniform(0.1, 0.3), random.uniform(0.5, 0.8)]
                    cust["velocity"] = [0.006, -0.003]

        # 4. Billing Queue Simulation (Handles queue growth and clearance)
        elif self.camera_id == "cam_04":
            # Periodically, the person at the front (queue_idx = 0) completes payment and exits
            if len(self.mock_customers) > 0 and random.random() < 0.015:
                # Remove index 0
                paid_cust = [c for c in self.mock_customers if c.get("queue_idx") == 0]
                if paid_cust:
                    self.mock_customers.remove(paid_cust[0])
                    # Move remaining shoppers forward
                    for c in self.mock_customers:
                        if "queue_idx" in c:
                            c["queue_idx"] -= 1
            
            # Periodically, a new shopper joins the end of the queue
            if len(self.mock_customers) < 6 and random.random() < 0.02:
                new_idx = len(self.mock_customers)
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [0.3 + new_idx * 0.12, 0.5 + random.uniform(-0.02, 0.02)],
                    "velocity": [0.0, 0.0],
                    "size": [50, 110],
                    "queue_idx": new_idx
                })
                
            # Smoothly transition shoppers positions based on queue index
            for c in self.mock_customers:
                if "queue_idx" in c:
                    target_x = 0.3 + c["queue_idx"] * 0.12
                    c["pos"][0] = c["pos"][0] * 0.9 + target_x * 0.1

        # 5. Billing Queue 2 Simulation (cam_05)
        elif self.camera_id == "cam_05":
            # Periodically, the person at the front (queue_idx = 0) completes payment and exits
            if len(self.mock_customers) > 0 and random.random() < 0.012:
                paid_cust = [c for c in self.mock_customers if c.get("queue_idx") == 0]
                if paid_cust:
                    self.mock_customers.remove(paid_cust[0])
                    for c in self.mock_customers:
                        if "queue_idx" in c:
                            c["queue_idx"] -= 1
            
            # Periodically, a new shopper joins the end of the queue
            if len(self.mock_customers) < 5 and random.random() < 0.018:
                new_idx = len(self.mock_customers)
                self.mock_customers.append({
                    "id": random.randint(100, 999),
                    "pos": [0.3 + new_idx * 0.12, 0.6 + random.uniform(-0.02, 0.02)],
                    "velocity": [0.0, 0.0],
                    "size": [50, 110],
                    "queue_idx": new_idx
                })
                
            # Smoothly transition positions
            for c in self.mock_customers:
                if "queue_idx" in c:
                    target_x = 0.3 + c["queue_idx"] * 0.12
                    c["pos"][0] = c["pos"][0] * 0.9 + target_x * 0.1
