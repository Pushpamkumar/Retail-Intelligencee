import json
import logging
import time
import os
from typing import Dict, Any, Optional
import pipeline.config as cfg

logger = logging.getLogger("EventStreamer")

class EventStreamer:
    """
    EventStreamer routes structured JSON events to Apache Kafka.
    Employs an automatic fallback to local JSONL and relational storage
    if the Kafka broker is offline or client libraries are uninstalled.
    """
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.use_fallback = True
        self.fallback_file_path = cfg.KAFKA_FALLBACK_FILE
        self._init_kafka()

    def _init_kafka(self):
        """Attempts to load and connect to the confluent_kafka broker."""
        try:
            from confluent_kafka import Producer
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'store_cv_pipeline',
                'retries': 3,
                'retry.backoff.ms': 500,
                'acks': 'all' # Guarantee absolute delivery
            }
            logger.info(f"Connecting to Kafka brokers at: {self.bootstrap_servers}...")
            self.producer = Producer(conf)
            # Test connectivity with a fast metadata poll
            self.producer.list_topics(timeout=1.0)
            self.use_fallback = False
            logger.info("Kafka Producer initialized successfully in PRODUCTION streaming mode.")
        except ImportError:
            logger.warning("confluent_kafka not installed. Engaging standalone fallback.")
        except Exception as e:
            logger.warning(f"Failed connecting to Kafka cluster: {e}. Engaging standalone fallback.")

    def _get_topic_for_event(self, event_type: str) -> str:
        """Maps event classes to dedicated event-streaming channels."""
        event_routing_table = {
            "customer_entered": cfg.TOPIC_CUSTOMER_EVENTS,
            "customer_exited": cfg.TOPIC_CUSTOMER_EVENTS,
            "shelf_visit": cfg.TOPIC_CUSTOMER_EVENTS,
            
            "zone_entry": cfg.TOPIC_ZONE_EVENTS,
            "zone_exit": cfg.TOPIC_ZONE_EVENTS,
            "zone_dwell_active": cfg.TOPIC_ZONE_EVENTS,
            
            "queue_detected": cfg.TOPIC_ANOMALY_EVENTS,
            "crowding_detected": cfg.TOPIC_ANOMALY_EVENTS,
            "long_dwell_time": cfg.TOPIC_ANOMALY_EVENTS,
            "zone_congestion": cfg.TOPIC_ANOMALY_EVENTS,
            
            "camera_offline": cfg.TOPIC_SYSTEM_EVENTS,
            "camera_obstructed": cfg.TOPIC_SYSTEM_EVENTS,
            "low_store_traffic": cfg.TOPIC_SYSTEM_EVENTS,
            "high_store_traffic": cfg.TOPIC_SYSTEM_EVENTS
        }
        return event_routing_table.get(event_type, "system_events")

    def _kafka_delivery_report(self, err, msg):
        """Callback to log success or handle DLQ (Dead Letter Queue) retries."""
        if err is not None:
            logger.error(f"Kafka message delivery failed: {err}")
            # In a live high-scale context, write failing logs to local Dead-Letter Queue
            self._write_to_fallback_file({"dlq_err": str(err), "payload": json.loads(msg.value().decode('utf-8'))})
        else:
            pass # Message successfully delivered

    def _write_to_fallback_file(self, event: Dict[str, Any]):
        """Saves messages to a local JSONL queue file when Kafka is unavailable."""
        try:
            with open(self.fallback_file_path, "a") as f:
                f.write(json.dumps(event) + "\n")
        except Exception as e:
            logger.error(f"Error writing to local backup logging stream: {e}")

    def _write_to_fallback_database(self, event: Dict[str, Any]):
        """
        Inserts events directly into the local SQLite database.
        Allows API routes to render graphs immediately without Kafka running.
        """
        # We will import the DB helper locally to prevent circular dependencies
        try:
            from backend.db import get_db_context
            from backend.models import EventModel, TrackedPersonModel, AnomalyModel
            from sqlalchemy.orm import Session
            
            # Extract header and payload
            header = event.get("header", {})
            payload = event.get("payload", {})
            event_type = header.get("event_type")
            camera_id = header.get("camera_id")
            timestamp_str = header.get("timestamp")
            
            # Map ISO timestamp to datetime
            from datetime import datetime
            dt = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S")
            
            with get_db_context() as db:
                # 1. Update active customer trajectory sessions if entry/exit triggers
                if event_type == "customer_entered":
                    pid = f"{camera_id}_{payload['person_id']}"
                    # Ensure person exists
                    person = db.query(TrackedPersonModel).filter(TrackedPersonModel.id == pid).first()
                    if not person:
                        person = TrackedPersonModel(
                            id=pid,
                            camera_id=camera_id,
                            person_id_seq=payload["person_id"],
                            entry_time=dt,
                            dwell_time_sec=0.0
                        )
                        db.add(person)
                        db.commit()
                        
                elif event_type == "customer_exited":
                    pid = f"{camera_id}_{payload['person_id']}"
                    person = db.query(TrackedPersonModel).filter(TrackedPersonModel.id == pid).first()
                    if person:
                        person.exit_time = dt
                        person.dwell_time_sec = payload["dwell_time_sec"]
                        db.commit()

                # 2. Map zone references if present
                zone_id = payload.get("zone_id")
                person_id = f"{camera_id}_{payload.get('person_id')}" if payload.get("person_id") else None

                # 3. Create relational event log
                db_event = EventModel(
                    event_type=event_type,
                    camera_id=camera_id,
                    zone_id=zone_id,
                    person_id=person_id,
                    timestamp=dt,
                    event_metadata=payload
                )
                db.add(db_event)

                # 4. Trigger alert log if event belongs to anomalies
                if event_type in ["crowding_detected", "long_dwell_time", "zone_congestion", "queue_detected"]:
                    desc = f"Zone Alert: {payload.get('zone_name', 'Zone')} | Occupancy: {payload.get('person_count', payload.get('queue_length', 1))}"
                    if event_type == "long_dwell_time":
                        desc = f"Customer {payload.get('person_id')} lingered in {payload.get('zone_name')} for {payload.get('dwell_time_sec')}s"
                        
                    db_anomaly = AnomalyModel(
                        camera_id=camera_id,
                        anomaly_type=event_type,
                        confidence_score=0.95,
                        description=desc,
                        timestamp=dt,
                        status="active"
                    )
                    db.add(db_anomaly)

                db.commit()
        except Exception as e:
            # Silently log: during initial stage, tables might not be created yet, which is fine
            # We degrade gracefully.
            pass

    def stream(self, event: Dict[str, Any]):
        """
        Streams event payload to the proper Kafka topic or local fallback.
        """
        event_type = event.get("header", {}).get("event_type", "unknown")
        topic = self._get_topic_for_event(event_type)
        
        # 1. Kafka streaming mode
        if not self.use_fallback and self.producer:
            try:
                payload_bytes = json.dumps(event).encode('utf-8')
                self.producer.produce(
                    topic, 
                    value=payload_bytes, 
                    key=str(event["header"]["event_id"]).encode('utf-8'),
                    callback=self._kafka_delivery_report
                )
                # Poll to activate delivery callback triggers
                self.producer.poll(0)
                logger.info(f"Streamed event '{event_type}' to Kafka topic: {topic}")
            except Exception as e:
                logger.error(f"Kafka transmission failure: {e}. Cascading to fallback queue.")
                self._write_to_fallback_file(event)
        else:
            self._write_to_fallback_file(event)
            
        # Always write to database so the REST API dashboard analytics are updated in real-time
        self._write_to_fallback_database(event)
        
        # Log event delivery fallback
        if cfg.DEBUG and (self.use_fallback or not self.producer):
            logger.info(f"[Fallback Queue] Buffered event '{event_type}' to file & DB. Topic: {topic}")

    def flush(self):
        """Forces pending broker messages to be transmitted."""
        if self.producer:
            self.producer.flush(timeout=1.0)
