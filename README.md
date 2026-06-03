# Purplle AI Sight - Store Intelligence Platform

[![Docker Support](https://img.shields.io/badge/Docker-Supported-blue.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-v0.100.0-009688.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![YOLOv11](https://img.shields.io/badge/YOLO-v11-FF8906.svg?logo=ultralytics&logoColor=white)](https://github.com/ultralytics/ultralytics)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB.svg?logo=python&logoColor=white)](https://www.python.org/)

**Purplle AI Sight** (Store Intelligence System) is a high-performance, edge-capable computer vision and retail analytics platform. It ingests video feeds from store CCTV cameras, tracks visitor spatial trajectories, identifies shelf dwell behaviors, and correlates these spatial insights with actual **Point-of-Sale (POS)** transactions to maximize store conversion rates and optimize brand product layouts.

---

## 🌟 Key Features

### 1. 🎥 Multi-Camera Spatial Processing (5 Channels)
Processes 5 concurrent camera channels covering the entire store layout:
- **`cam_01` (Main Entrance)**: Measures store footfall waves and peak entry hours.
- **`cam_02` (Cosmetics Aisle)**: Tracks browsing duration for brands like Maybelline, Faces Canada, Lakme, and NY Bae.
- **`cam_03` (Skincare Island)**: Tracks engagement for Minimalist, Aqualogica, Foxtale, and Juicy Chemistry.
- **`cam_04` (Cash Counter 1)** & **`cam_05` (Cash Counter 2)**: Monitors checkout lanes, queue counts, and waiting congestion metrics.

*Features an optimized 3-frame skipping strategy to run all 5 pipelines concurrently on standard host CPUs.*

### 2. 📊 CCTV-to-POS Conversion Analytics
Bridges spatial browsing data (CCTV visits and dwell time) with actual transaction logs. 
- Mapped products to their physical shelves.
- Automatically calculates **Shelf Conversion Rates** (`POS Invoices / CCTV Visits %`) per brand counter.
- Detects **Operational Friction**: Triggers alerts if a brand shelf has high dwell time but low sales (indicating out-of-stock items, pricing issues, or layout layout mismatches).

### 3. 🗺️ Interactive shop floor map
Renders a live CAD floor plan representing the **Revised Store Layout**:
- Visualizes brand placements along top and bottom walls and center units.
- Overlays real-time occupant density counters on each shelf (powered by WebSockets).
- Clicking a shelf pulls a sidebar details pane showing footfall, revenue, average dwell, and conversion rates.

### 4. 📈 A/B Layout Lift Analysis
Exposes financial simulation metrics comparing the **Current Layout** vs. the **Revised Layout**:
- Compares brand swaps (e.g. replacing low-velocity Pilgrim/Dot & Key shelves with Foxtale/Juicy Chemistry).
- Measures the impact of category adjacencies (e.g. Alps Goodness sales lift by placing it next to the new Mens Care section).
- Exhibits a calculated **revenue lift** (+3.0% net sales lift) and hourly correlation curves.

---

## 🏗️ System Flow & Architecture Design

The system runs two distinct pipelines (CCTV Spatial Analytics and POS Transactions) that merge inside the relational database, providing correlated real-time business insights.

### Data Flow Diagram

```mermaid
graph TD
    subgraph sg1 ["1. CCTV Spatial Pipeline (OS Daemon Threads)"]
        C1["CCTV streams / Mock Feeds"] -->|Frame Buffers| VI["Video Ingest Thread"]
        VI -->|Raw Video Frames| DET["YOLOv11 Person Detector"]
        DET -->|Person Bounding Boxes| TRK["IOU Target Tracker"]
        TRK -->|Visitor Trajectories & Centroids| ZA["Zone Analyzer"]
        ZA -->|Ray-Casting Polygons Test| ZA_Events{"Events Evaluator"}
        ZA_Events -->|Zone Entry / Exit| EG["Event Generator"]
        ZA_Events -->|Dwell Coordinates| HE["Heatmap Engine"]
        EG -->|Structured JSON Logs| ES["Event Streamer"]
    end

    subgraph sg2 ["2. POS Sales Pipeline (System Seeder)"]
        CSV1["Brigade_Bangalore_10_April_26.csv"] -->|Startup Parser| SD["DB Seeder Function"]
        CSV2["POS_sample_transactions.csv"] -->|Startup Parser| SD
    end

    subgraph sg3 ["3. Storage & Persistence"]
        ES -->|Fallback DB Writes| DB[("PostgreSQL / SQLite")]
        SD -->|Bulk DB Inserts| DB
    end

    subgraph sg4 ["4. API & Real-time Web Dashboard"]
        DB -->|ORM Analytics Queries| API["FastAPI Web Server"]
        ZA -->|High-Freq Occupancy Feed| WS["WebSockets Telemetry Loop"]
        API -->|REST Endpoints / JSON| Dashboard["HTML5 UI Dashboard"]
        WS -->|5Hz Telemetry Update| Dashboard
    end
```

### How the Flows Work

#### A. CCTV Spatial Pipeline
1. **Frame Ingestion**: `VideoIngester` pulls raw frames from RTSP camera streams (or simulated mock feeds) into a thread-safe frame queue.
2. **AI Detection**: Every 3rd frame is sent to the `YOLOv11n` inference engine to identify people (`class 0`). The remaining frames reuse the detection bounding boxes to save 66% CPU capacity.
3. **Centroid Tracking**: Bounding boxes are grouped into persistent unique customer IDs using an Intersection-Over-Union (IOU) tracking tracker.
4. **Spatial Polygon Test**: Centroids are mapped against normalized polygons representing physical brand shelves using `cv2.pointPolygonTest`. 
5. **Business Event Generation**: If a customer stays inside a zone for $>5$ seconds, a `shelf_visit` event is emitted. If they stay $>20$ seconds, a `long_dwell_time` is triggered.
6. **Persistence**: Events are saved to `store_intelligence.db` or streamed to Kafka topics.

#### B. POS Sales Pipeline
1. **Startup Boot**: When FastAPI starts, the system checks the `pos_transactions` table.
2. **CSV Seeding**: If empty, the backend parses the transaction CSV datasets, links the items to their brands (e.g. Good Vibes, Faces Canada), and saves them to the database.
3. **Correlation Engine**: The API cross-references CCTV shelf-visit records with the sales table to determine the **checkout conversion rate** per shelf.

---

## 🚀 How to Start

### Method A: Docker Compose (Recommended)
This spins up the entire retail stack, including the FastAPI backend server, PostgreSQL database, Apache Kafka, Prometheus, and Grafana.

1. Ensure Docker Desktop is running.
2. Run the compose build command:
   ```bash
   docker compose up --build
   ```
3. Open `http://localhost:8000/` in your browser.

### Method B: Standalone Local Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/Pushpamkumar/Retail-Intelligence.git
   cd Retail-Intelligence
   ```

2. **Install requirements:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start the server:**
   ```bash
   python -m backend.main
   ```
   *The server will create a local SQLite database (`store_intelligence.db`) and automatically seed 202 transactions from the CSV files.*

4. **Navigate to the Dashboard:**
   Open `http://localhost:8000/` in your browser.

---

## ⚙️ Configuration (.env)
Create a `.env` file in the root folder to configure stream overrides or database endpoints:
```env
# ENV overrides (development / production)
ENV=development
DEBUG=true

# CCTV Streams overrides (RTSP link or local .mp4 filepath)
STREAM_CAM_01=mock://entrance
STREAM_CAM_02=mock://cosmetics
STREAM_CAM_03=mock://skincare
STREAM_CAM_04=mock://billing
STREAM_CAM_05=mock://billing2
```

---

## 📂 Project Structure
- `backend/`: FastAPI routes, database ORM models, schemas, and analytics engine.
- `backend/static/index.html`: Interactive glassmorphism HTML5 dashboard.
- `pipeline/`: Computer Vision pipelines (YOLO detector, IOU tracker, zone ray-casting, event publisher).
- `database/init.sql`: SQL initialization script.
- `Brigade_Bangalore_10_April_26 (1)bc6219c.csv`: POS transaction database source.
