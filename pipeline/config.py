import os
import json

# Try to load environment variables from a .env file in the workspace root if it exists
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
if os.path.exists(env_path):
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    os.environ[key.strip()] = val.strip().strip('"').strip("'")
    except Exception as e:
        print(f"Warning: Failed to load .env file: {e}")

# ==========================================
# Core Settings
# ==========================================
DEBUG = os.getenv("DEBUG", "true").lower() == "true"
ENVIRONMENT = os.getenv("ENV", "development") # development, production

# ==========================================
# Database Configurations
# ==========================================
# Production PostgreSQL URI or fallback SQLite local database
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "store_intelligence")

POSTGRES_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# Auto-fallback to local sqlite file inside the app data or workspace folder
SQLITE_URL = "sqlite:///./store_intelligence.db"

# ==========================================
# Kafka Settings
# ==========================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_FALLBACK_FILE = "./kafka_fallback_events.jsonl"

# Topics
TOPIC_CUSTOMER_EVENTS = "customer_events"
TOPIC_ZONE_EVENTS = "zone_events"
TOPIC_ANOMALY_EVENTS = "anomaly_events"
TOPIC_SYSTEM_EVENTS = "system_events"

# ==========================================
# Camera Settings
# ==========================================
CAMERAS = {
    "cam_01": {
        "name": "Store Main Entrance",
        "location": "Entrance Vestibule",
        "stream_url": os.getenv("STREAM_CAM_01", "mock://entrance"),
        "fps": 30,
        "width": 1280,
        "height": 720
    },
    "cam_02": {
        "name": "Cosmetics Counter View",
        "location": "Aisle 3 Cosmetics",
        "stream_url": os.getenv("STREAM_CAM_02", "mock://cosmetics"),
        "fps": 30,
        "width": 1280,
        "height": 720
    },
    "cam_03": {
        "name": "Skincare Island",
        "location": "Aisle 4 Skincare",
        "stream_url": os.getenv("STREAM_CAM_03", "mock://skincare"),
        "fps": 30,
        "width": 1280,
        "height": 720
    },
    "cam_04": {
        "name": "Billing Checkout Line 1",
        "location": "Checkout Counter 1",
        "stream_url": os.getenv("STREAM_CAM_04", "mock://billing"),
        "fps": 30,
        "width": 1280,
        "height": 720
    },
    "cam_05": {
        "name": "Billing Checkout Line 2",
        "location": "Checkout Counter 2",
        "stream_url": os.getenv("STREAM_CAM_05", "mock://billing2"),
        "fps": 30,
        "width": 1280,
        "height": 720
    }
}

# ==========================================
# Zone Layout Configuration
# ==========================================
# Coordinates are normalized (0.0 to 1.0) and mapped to width & height in the processor
ZONES = {
    "cam_01": [
        {
            "id": "zone_entrance",
            "name": "Entrance Vestibule",
            "polygon": [[0.0, 0.4], [1.0, 0.4], [1.0, 0.9], [0.0, 0.9]]
        }
    ],
    "cam_02": [
        {
            "id": "zone_cosmetics",
            "name": "Cosmetics Section",
            "polygon": [[0.1, 0.1], [0.5, 0.1], [0.5, 0.8], [0.1, 0.8]]
        }
    ],
    "cam_03": [
        {
            "id": "zone_skincare",
            "name": "Skincare Section",
            "polygon": [[0.4, 0.1], [0.9, 0.1], [0.9, 0.8], [0.4, 0.8]]
        }
    ],
    "cam_04": [
        {
            "id": "zone_billing",
            "name": "Billing Counter Queue",
            "polygon": [[0.2, 0.3], [0.8, 0.3], [0.8, 0.7], [0.2, 0.7]]
        }
    ],
    "cam_05": [
        {
            "id": "zone_billing_2",
            "name": "Billing Counter Queue 2",
            "polygon": [[0.2, 0.3], [0.8, 0.3], [0.8, 0.7], [0.2, 0.7]]
        }
    ]
}

# ==========================================
# Inference / CV Hyperparameters
# ==========================================
YOLO_MODEL_PATH = "yolo11n.pt" # Auto-downloads on first run
CONFIDENCE_THRESHOLD = 0.4
IOU_THRESHOLD = 0.45
MAX_TRACK_AGE_FRAMES = 30 # Number of frames to hold a lost target before cleanup
DWELL_TIME_THRESHOLD_SEC = 20.0 # Time in a zone to trigger "long dwell time" alert
CROWDING_THRESHOLD = 5 # Number of simultaneous people in a zone to trigger crowding event
QUEUE_LONG_THRESHOLD = 4 # Number of people in billing zone to trigger queue alarm
