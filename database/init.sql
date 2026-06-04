-- ==========================================
-- Store Intelligence System PostgreSQL Schema
-- ==========================================

-- Clean up existing tables
DROP TABLE IF EXISTS analytics CASCADE;
DROP TABLE IF EXISTS anomalies CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS tracked_persons CASCADE;
DROP TABLE IF EXISTS zones CASCADE;
DROP TABLE IF EXISTS cameras CASCADE;
DROP TABLE IF EXISTS pos_transactions CASCADE;

-- 1. Cameras Table
CREATE TABLE cameras (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100) NOT NULL,
    stream_url VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'offline', -- active, offline, error
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Zones Table (Configurable coordinates within a camera view)
CREATE TABLE zones (
    id VARCHAR(50) PRIMARY KEY,
    camera_id VARCHAR(50) NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    polygon_coordinates JSONB NOT NULL, -- Array of points [[x1, y1], [x2, y2], ...] normalized
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Tracked Persons Table (Tracks customer session metrics)
CREATE TABLE tracked_persons (
    id VARCHAR(100) PRIMARY KEY, -- Combines camera_id + person_id_seq + timestamp
    camera_id VARCHAR(50) NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
    person_id_seq INTEGER NOT NULL,
    entry_time TIMESTAMP WITH TIME ZONE NOT NULL,
    exit_time TIMESTAMP WITH TIME ZONE,
    dwell_time_sec DOUBLE PRECISION DEFAULT 0.0
);

-- 4. Events Table (Business events and logs)
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL, -- customer_entered, customer_exited, shelf_visit, queue_detected, crowding_detected, etc.
    camera_id VARCHAR(50) NOT NULL REFERENCES cameras(id) ON DELETE CASCADE,
    zone_id VARCHAR(50) REFERENCES zones(id) ON DELETE SET NULL,
    person_id VARCHAR(100) REFERENCES tracked_persons(id) ON DELETE SET NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB DEFAULT '{}'::jsonb
);

-- 5. Anomalies Table (Security or Operational concerns)
CREATE TABLE anomalies (
    id BIGSERIAL PRIMARY KEY,
    camera_id VARCHAR(50) REFERENCES cameras(id) ON DELETE SET NULL,
    anomaly_type VARCHAR(50) NOT NULL, -- camera_obstructed, crowding, queue_backup, sudden_traffic_drop
    confidence_score DOUBLE PRECISION NOT NULL,
    description TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'active' -- active, resolved, ignored
);

-- 6. Analytics Table (Pre-aggregated rollups for analytics queries)
CREATE TABLE analytics (
    id BIGSERIAL PRIMARY KEY,
    metric_type VARCHAR(50) NOT NULL, -- footfall_hourly, footfall_daily, zone_occupancy_avg, queue_dwell_avg, performance_score
    dimension VARCHAR(100) NOT NULL,  -- e.g., "cosmetics_section", "2026-05-31:10"
    value DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSONB DEFAULT '{}'::jsonb
);

-- 7. POS Transactions Table (Sales item details)
CREATE TABLE pos_transactions (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE,
    basket_value_inr DOUBLE PRECISION,
    coupon_code VARCHAR(100),
    offer_name VARCHAR(200),
    discount_code VARCHAR(100),
    invoice_number VARCHAR(100),
    invoice_type VARCHAR(50),
    order_date VARCHAR(50) NOT NULL,
    order_time VARCHAR(50) NOT NULL,
    store_id VARCHAR(50) NOT NULL,
    store_name VARCHAR(100),
    city VARCHAR(100),
    customer_name VARCHAR(100),
    customer_number VARCHAR(50),
    sku VARCHAR(100),
    product_id VARCHAR(50) NOT NULL,
    ean VARCHAR(50),
    product_name VARCHAR(255),
    brand_name VARCHAR(100) NOT NULL,
    dep_name VARCHAR(100),
    sub_category VARCHAR(100),
    brand_type VARCHAR(50),
    qty INTEGER DEFAULT 1,
    gmv DOUBLE PRECISION DEFAULT 0.0,
    nmv DOUBLE PRECISION DEFAULT 0.0,
    coupon_amount DOUBLE PRECISION DEFAULT 0.0,
    item_promotion DOUBLE PRECISION DEFAULT 0.0,
    amt_without_gwp DOUBLE PRECISION DEFAULT 0.0,
    total_amount DOUBLE PRECISION DEFAULT 0.0
);

CREATE INDEX idx_pos_transactions_brand ON pos_transactions(brand_name);
CREATE INDEX idx_pos_transactions_order ON pos_transactions(order_id);

-- ==========================================
-- Database Indexes for Sub-50ms Query Performance
-- ==========================================

-- Index on events for historical timeline filtering & streaming pagination
CREATE INDEX idx_events_timestamp_type ON events(timestamp DESC, event_type);
CREATE INDEX idx_events_camera_zone ON events(camera_id, zone_id);
CREATE INDEX idx_events_person ON events(person_id);

-- Index on anomalies for immediate alerting on active alerts
CREATE INDEX idx_anomalies_active ON anomalies(timestamp DESC) WHERE status = 'active';

-- Index on analytics for rolling analytics dashboard requests
CREATE INDEX idx_analytics_type_time ON analytics(metric_type, timestamp DESC);

-- Index on tracked persons for active shopper session reports
CREATE INDEX idx_tracked_persons_entry ON tracked_persons(entry_time DESC);

-- ==========================================
-- Production Data Retention Strategy
-- ==========================================
-- A standard partition scheme or a scheduled store-procedure retention routine is optimal.
-- The procedure below purges detailed events, tracks, and logs beyond a configurable period (e.g., 30 days),
-- keeping aggregated analytics metrics permanently.

CREATE OR REPLACE FUNCTION cleanup_old_data(days_to_keep INT) 
RETURNS VOID AS $$
BEGIN
    -- Delete old events
    DELETE FROM events WHERE timestamp < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    
    -- Delete old anomalies
    DELETE FROM anomalies WHERE timestamp < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
    
    -- Delete old tracked persons that exited
    DELETE FROM tracked_persons 
    WHERE exit_time IS NOT NULL 
      AND exit_time < CURRENT_TIMESTAMP - (days_to_keep || ' days')::INTERVAL;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- Initial Seeding of Store Layout & Configuration
-- ==========================================

-- Seed Cameras
INSERT INTO cameras (id, name, location, stream_url, status) VALUES
('cam_01', 'Store Main Entrance', 'Entrance Vestibule', 'rtsp://admin:admin123@192.168.1.101:554/stream1', 'active'),
('cam_02', 'Cosmetics Counter View', 'Aisle 3 Cosmetics', 'rtsp://admin:admin123@192.168.1.102:554/stream1', 'active'),
('cam_03', 'Skincare Island', 'Aisle 4 Skincare', 'rtsp://admin:admin123@192.168.1.103:554/stream1', 'active'),
('cam_04', 'Billing Checkout Line', 'Checkout Counter', 'rtsp://admin:admin123@192.168.1.104:554/stream1', 'active'),
('cam_05', 'Haircare & Aisle View', 'Aisle 5 Haircare', 'rtsp://admin:admin123@192.168.1.105:554/stream1', 'active');

-- Seed Store Zones (Coordinates are normalized polygon corners between 0.0 and 1.0)
INSERT INTO zones (id, camera_id, name, polygon_coordinates) VALUES
('zone_entrance', 'cam_01', 'Entrance Vestibule', '[[0.0, 0.4], [1.0, 0.4], [1.0, 0.9], [0.0, 0.9]]'),
('zone_cosmetics', 'cam_02', 'Cosmetics Section', '[[0.1, 0.1], [0.5, 0.1], [0.5, 0.8], [0.1, 0.8]]'),
('zone_skincare', 'cam_03', 'Skincare Section', '[[0.4, 0.1], [0.9, 0.1], [0.9, 0.8], [0.4, 0.8]]'),
('zone_billing', 'cam_04', 'Billing Counter Queue', '[[0.2, 0.3], [0.8, 0.3], [0.8, 0.7], [0.2, 0.7]]'),
('zone_haircare', 'cam_05', 'Haircare Section', '[[0.3, 0.3], [0.7, 0.3], [0.7, 0.7], [0.3, 0.7]]');
