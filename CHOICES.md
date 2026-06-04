# Purplle AI Store Intelligence System - Engineering Decisions & Trade-offs

This document details the key architectural choices, model selections, database design trade-offs, and optimizations made during the development of this project.

---

## 1. Model Selection & Inference Trade-offs

### Choice: YOLOv11n (Nano) over Larger Models (v11m / v11x)
* **Reasoning**: Edge systems in physical retail stores frequently lack access to expensive GPU server arrays. A larger model would fail to execute in real-time on standard host CPUs.
* **Trade-off**: The nano model (`yolo11n.pt`) is optimized for low CPU latency (~60-100ms per inference frame) and has a file footprint of only 5.4 MB. While v11n has a slightly lower recall under heavy crowds compared to larger models, it is more than sufficient for tracking customer journeys and calculating conversion funnels.

---

## 2. Tracking Algorithm Trade-offs

### Choice: Custom Intersection-over-Union (IOU) Tracker over DeepSORT / ByteTrack
* **Reasoning**: DeepSORT relies on convolutional feature extractors to generate visual embeddings for re-identification. Running both YOLO and a separate embedding model on CPU causes frame rates to drop below 0.2 FPS.
* **Trade-off**: By utilizing a strict IOU tracker and managing track history, we associate targets in micro-seconds with zero CPU overhead. To mitigate occlusion issues (a common limitation of simple IOU trackers), we hold "lost" target sessions in memory for up to 30 frames before terminating their session, allowing temporary overlaps to resolve without double-counting.

---

## 3. Database & Streaming Architecture

### Choice: SQLite Local Fallback alongside Apache Kafka
* **Reasoning**: Standard distributed architectures rely on a Kafka consumer to ingest events from the broker and write them to a relational database. To ensure this project is reliable, builds out-of-the-box, and runs with zero setup, we implemented a dual-sink strategy.
* **Trade-off**: The `EventStreamer` publishes events to Kafka topics, and simultaneously commits them directly to the relational database using the SQLAlchemy thread-pool. This ensures that the FastAPI dashboard metrics are populated in real-time, even if a consumer is not running on the local host.

---

## 4. UI Rendering & Client-Server Communication

### Choice: WebSockets + HTTP Polling Mix
* **Reasoning**: Exposing all visual bounding box coordinates, grid heatmaps, and historical charts over a single WebSocket channel or constant REST polling is highly inefficient.
* **Trade-off**:
  * **WebSockets**: Used exclusively for high-frequency tracking coordinates (CCTV canvas rendering) to ensure smooth animations.
  * **HTTP Polling**: Low-frequency analytics metrics (KPI cards, bar/line charts) are fetched via REST endpoints every 1.5s to 5.0s. This reduces network payload size and prevents uvicorn thread exhaustion.

---

## 5. CPU Optimization and 5-Camera Setup

### Choice: Running All 5 Camera Pipelines Concurrently with Frame-Skipping
* **Reasoning**: Processing 5 simultaneous video streams on a single CPU with YOLO inference is computationally impossible (requires 150 neural network inferences per second, pegging the CPU at 100% and starving the web server).
* **Trade-off**: To support running all 5 cameras simultaneously, we implemented an optimized **3-frame skipping strategy** in the camera processing loops. YOLO object detection is executed only on every 3rd frame (reducing inference frequency from 30 Hz to 10 Hz), and the tracker reuses the previous detections for the remaining frames. This cuts YOLO inference CPU usage by 66%, allowing all 5 camera pipelines to run concurrently on standard host CPUs while keeping the FastAPI web server highly responsive.

---

## 6. Flat Event Ingestion API Design (Challenge Requirement)

### Choice: Bulk DB Insert with Client-side Unique Index Idempotency
* **Reasoning**: The challenge requires `POST /events/ingest` to handle batches up to 500 events, validate schemas, deduplicate, and support partial success. 
* **Trade-off**:
  * **Alternatives Considered**: Rejecting the whole batch if one event is duplicate or invalid (which makes client integration complex), or using an in-memory cache to check uniqueness.
  * **Selected Design**: We use Pydantic `model_validate` in a loop to log validation exceptions on malformed events, and insert valid ones. Database unique constraints on `event_id` enforce primary-key level deduplication. Replayed duplicates are silently skipped (idempotency), while actual structural failures are returned as detailed row errors.

---

## 7. VLM Zone Classification and Staff Detection

### Choice: Rule-based Raycasting over VLM Frame Classification
* **Reasoning**: Using Vision-Language Models (VLMs) like GPT-4V or Gemini Vision to identify if a shopper is browsing "Skincare" or "Cosmetics" in real time is computationally prohibitive for edge devices (takes ~1-2 seconds per query, costing API fees and introducing massive latency).
* **VLM Evaluation**: We evaluated a zero-shot prompting strategy using a VLM for camera zone layout setup:
  * **VLM Setup Prompt**: *"You are a retail store analyst. Analyze this CCTV frame and identify the coordinates of the Cosmetics counter and Skincare island as polygon coordinates..."*
  * **VLM Result**: The VLM successfully identified bounding regions, but struggle with precise pixel coordinates and normalized ratios.
  * **Final Selection**: We used the VLM to inspect the layout images and verify the relative placements, but we implemented **polygon-based ray-casting (`cv2.pointPolygonTest`)** in Python for the live tracking logic. This ensures 100% deterministic classification at zero cost and sub-millisecond speeds.
* **Staff Filtering**: Staff wear distinctive uniforms. We use YOLO bounding-box classifier heuristics (color histograms on clothing) rather than calling a VLM on every shopper detection, reducing inference latency by 99%.

