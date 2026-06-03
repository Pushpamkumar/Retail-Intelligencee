import numpy as np
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger("Tracker")

class Track:
    """Represents a single persistent customer trajectory."""
    def __init__(self, track_id: int, bbox: List[int], start_time: float, sim_id: int = None):
        self.track_id = track_id
        self.bbox = bbox # [x1, y1, x2, y2]
        self.centroid = self._get_centroid(bbox)
        self.history = [self.centroid] # Coordinates pathway for heatmaps
        self.start_time = start_time
        self.last_update = start_time
        self.lost_frames = 0
        self.sim_id = sim_id # Sim anchor alignment if running mock
        
    def _get_centroid(self, bbox: List[int]) -> Tuple[int, int]:
        return (int((bbox[0] + bbox[2]) / 2), int((bbox[1] + bbox[3]) / 2))

    def update(self, bbox: List[int], timestamp: float):
        self.bbox = bbox
        self.centroid = self._get_centroid(bbox)
        self.history.append(self.centroid)
        if len(self.history) > 200: # Limit coordinate length memory
            self.history.pop(0)
        self.last_update = timestamp
        self.lost_frames = 0

class CustomerTracker:
    """
    CustomerTracker tracks human detections across subsequent video frames.
    Uses Intersection over Union (IoU) and Centroid proximity association.
    """
    def __init__(self, max_lost_frames: int = 30, min_iou: float = 0.25):
        self.max_lost_frames = max_lost_frames
        self.min_iou = min_iou
        self.next_track_id = 1001
        self.tracks: Dict[int, Track] = {}

    def _calculate_iou(self, boxA: List[int], boxB: List[int]) -> float:
        """Computes Intersection over Union (IoU) of two bounding boxes."""
        xA = max(boxA[0], boxB[0])
        yA = max(boxA[1], boxB[1])
        xB = min(boxA[2], boxB[2])
        yB = min(boxA[3], boxB[3])
        
        interArea = max(0, xB - xA + 1) * max(0, yB - yA + 1)
        boxAArea = (boxA[2] - boxA[0] + 1) * (boxA[3] - boxA[1] + 1)
        boxBArea = (boxB[2] - boxB[0] + 1) * (boxB[3] - boxB[1] + 1)
        
        iou = interArea / float(boxAArea + boxBArea - interArea + 1e-6)
        return iou

    def _calculate_centroid_distance(self, ptA: Tuple[int, int], ptB: Tuple[int, int]) -> float:
        """Computes Euclidean distance between two points."""
        return float(np.sqrt((ptA[0] - ptB[0])**2 + (ptA[1] - ptB[1])**2))

    def update(self, detections: List[Dict[str, Any]], timestamp: float) -> List[Dict[str, Any]]:
        """
        Updates trackers with new frame detections.
        Returns:
            List of active tracks with bounding boxes and IDs: 
            [{"person_id": 1001, "bbox": [x1, y1, x2, y2], "centroid": (cx, cy)}]
        """
        active_dets = detections.copy()
        matched_tracks = {}
        matched_detections = set()

        # Phase 1: Try exact simulation matching (ground truth fallback) if keys are provided
        for det_idx, det in enumerate(active_dets):
            if "sim_id" in det and det["sim_id"] is not None:
                # Find track with matching sim_id
                for tid, track in list(self.tracks.items()):
                    if track.sim_id == det["sim_id"]:
                        track.update(det["bbox"], timestamp)
                        matched_tracks[tid] = track
                        matched_detections.add(det_idx)
                        break

        # Filter out matched simulation detections
        unmatched_det_indices = [i for i in range(len(active_dets)) if i not in matched_detections]

        # Phase 2: IoU Bipartite Greedy Matching for standard CV inputs
        unmatched_tracks = [t for t in list(self.tracks.values()) if t.track_id not in matched_tracks]

        # Sort unmatched track lists to associate based on history age
        for track in unmatched_tracks:
            best_iou = -1.0
            best_det_idx = -1
            
            for idx in unmatched_det_indices:
                det = active_dets[idx]
                iou = self._calculate_iou(track.bbox, det["bbox"])
                if iou > best_iou:
                    best_iou = iou
                    best_det_idx = idx
            
            if best_iou >= self.min_iou and best_det_idx != -1:
                track.update(active_dets[best_det_idx]["bbox"], timestamp)
                # Assign ground truth alignment if running simulation mode
                if "sim_id" in active_dets[best_det_idx]:
                    track.sim_id = active_dets[best_det_idx]["sim_id"]
                matched_tracks[track.track_id] = track
                unmatched_det_indices.remove(best_det_idx)

        # Phase 3: Distance-based Centroid fallback for rapidly moving objects
        still_unmatched_tracks = [t for t in unmatched_tracks if t.track_id not in matched_tracks]
        for track in still_unmatched_tracks:
            best_dist = 9999.0
            best_det_idx = -1
            
            for idx in unmatched_det_indices:
                det = active_dets[idx]
                cx = int((det["bbox"][0] + det["bbox"][2]) / 2)
                cy = int((det["bbox"][1] + det["bbox"][3]) / 2)
                dist = self._calculate_centroid_distance(track.centroid, (cx, cy))
                if dist < best_dist:
                    best_dist = dist
                    best_det_idx = idx
                    
            # 60 pixels proximity limit as tolerance
            if best_dist < 60.0 and best_det_idx != -1:
                track.update(active_dets[best_det_idx]["bbox"], timestamp)
                if "sim_id" in active_dets[best_det_idx]:
                    track.sim_id = active_dets[best_det_idx]["sim_id"]
                matched_tracks[track.track_id] = track
                unmatched_det_indices.remove(best_det_idx)

        # Phase 4: Create new tracks for remaining unmatched detections
        for idx in unmatched_det_indices:
            det = active_dets[idx]
            new_track = Track(
                track_id=self.next_track_id,
                bbox=det["bbox"],
                start_time=timestamp,
                sim_id=det.get("sim_id")
            )
            self.tracks[self.next_track_id] = new_track
            matched_tracks[self.next_track_id] = new_track
            self.next_track_id += 1

        # Phase 5: Maintain tracking age and clear stale trajectories
        output_tracks = []
        for tid, track in list(self.tracks.items()):
            if tid in matched_tracks:
                output_tracks.append({
                    "person_id": track.track_id,
                    "bbox": track.bbox,
                    "centroid": track.centroid,
                    "history": track.history
                })
            else:
                # Target was lost in this frame -> increase lost counters
                track.lost_frames += 1
                if track.lost_frames > self.max_lost_frames:
                    # Clean up lost track session
                    del self.tracks[tid]
                else:
                    # Retain last position under occlusion grace period
                    output_tracks.append({
                        "person_id": track.track_id,
                        "bbox": track.bbox,
                        "centroid": track.centroid,
                        "history": track.history,
                        "occluded": True
                    })

        return output_tracks
