from typing import List, Tuple, Dict, Any, Set
import time
import logging

logger = logging.getLogger("ZoneAnalyzer")

class ZoneAnalyzer:
    """
    ZoneAnalyzer detects customer positions within geometric polygons.
    Computes Zone Entry, Zone Exit, Occupancy, and Dwell times.
    """
    def __init__(self, camera_id: str, zone_configs: List[Dict[str, Any]], frame_width: int = 1280, frame_height: int = 720):
        self.camera_id = camera_id
        self.frame_width = frame_width
        self.frame_height = frame_height
        
        # Parse and scale zone polygon coordinates to absolute pixel space
        self.zones = {}
        for z in zone_configs:
            scaled_poly = [
                (int(pt[0] * frame_width), int(pt[1] * frame_height))
                for pt in z["polygon"]
            ]
            self.zones[z["id"]] = {
                "name": z["name"],
                "polygon": scaled_poly,
                "active_persons": set(), # Set of active person_ids in the zone
                "entry_timestamps": {}   # Dict mapping person_id -> entry_time
            }

    def _point_in_polygon(self, x: int, y: int, polygon: List[Tuple[int, int]]) -> bool:
        """
        Ray-Casting Algorithm for Point-in-Polygon (PIP) testing.
        Determines if point (x, y) is inside the polygon.
        """
        n = len(polygon)
        inside = False
        p1x, p1y = polygon[0]
        
        for i in range(n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
            
        return inside

    def analyze(self, active_tracks: List[Dict[str, Any]], timestamp: float) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """
        Analyzes active customer positions against zones.
        Args:
            active_tracks: Output from tracker update.
            timestamp: Unix epoch timestamp in seconds.
        Returns:
            Tuple of:
               - List of zone events generated: [{"event_type": "zone_entry", "person_id": 1001, "zone_id": "zone_1"}]
               - Dictionary of current occupancy counts per zone: {"zone_entrance": 2}
        """
        events = []
        occupancy_counts = {}

        # Scan each zone
        for zone_id, zone_data in self.zones.items():
            current_occupants = set()
            polygon = zone_data["polygon"]
            active_persons = zone_data["active_persons"]
            entry_timestamps = zone_data["entry_timestamps"]

            # Check if active tracks are inside this zone
            for track in active_tracks:
                person_id = track["person_id"]
                cx, cy = track["centroid"]
                
                # Check occlusion grace (keep them in zone if temporarily occluded)
                is_occluded = track.get("occluded", False)
                
                if is_occluded:
                    # If previously inside, remain inside during brief tracking gap
                    if person_id in active_persons:
                        current_occupants.add(person_id)
                    continue

                # Run Ray-Casting Point-in-Polygon test
                is_inside = self._point_in_polygon(cx, cy, polygon)

                if is_inside:
                    current_occupants.add(person_id)
                    
                    # 1. Detect Zone Entry
                    if person_id not in active_persons:
                        active_persons.add(person_id)
                        entry_timestamps[person_id] = timestamp
                        
                        logger.info(f"Person {person_id} entered zone: {zone_data['name']}")
                        events.append({
                            "event_type": "zone_entry",
                            "person_id": person_id,
                            "camera_id": self.camera_id,
                            "zone_id": zone_id,
                            "zone_name": zone_data["name"],
                            "timestamp": timestamp,
                            "bbox": track["bbox"]
                        })
                    else:
                        # 2. Maintain inside state & check for dwell-time updates (useful for shelf visits)
                        dwell_time = timestamp - entry_timestamps[person_id]
                        # We can emit periodic zone occupancy pulse/updates
                        events.append({
                            "event_type": "zone_dwell_active",
                            "person_id": person_id,
                            "camera_id": self.camera_id,
                            "zone_id": zone_id,
                            "zone_name": zone_data["name"],
                            "timestamp": timestamp,
                            "dwell_time_sec": round(dwell_time, 2),
                            "bbox": track["bbox"]
                        })
                        
            # 3. Detect Zone Exits (People who were inside but are no longer present)
            exited_persons = active_persons - current_occupants
            for person_id in list(exited_persons):
                entry_time = entry_timestamps.pop(person_id, timestamp)
                dwell_time = timestamp - entry_time
                active_persons.remove(person_id)
                
                logger.info(f"Person {person_id} exited zone: {zone_data['name']} (Dwell: {dwell_time:.1f}s)")
                events.append({
                    "event_type": "zone_exit",
                    "person_id": person_id,
                    "camera_id": self.camera_id,
                    "zone_id": zone_id,
                    "zone_name": zone_data["name"],
                    "timestamp": timestamp,
                    "dwell_time_sec": round(dwell_time, 2)
                })

            # Record final occupancy KPI
            occupancy_counts[zone_id] = len(current_occupants)

        return events, occupancy_counts
