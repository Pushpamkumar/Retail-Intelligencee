import numpy as np
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger("HeatmapEngine")

class HeatmapEngine:
    """
    HeatmapEngine tracks spatial trajectories and aggregates point coordinate density.
    Translates pixel positions into normalized 2D grid counts to classify hot/cold zones.
    """
    def __init__(self, width: int = 1280, height: int = 720, grid_cols: int = 32, grid_rows: int = 18):
        self.width = width
        self.height = height
        self.grid_cols = grid_cols
        self.grid_rows = grid_rows
        
        # Grid density matrix representing shopper dwell intensity
        self.density_grid = np.zeros((grid_rows, grid_cols), dtype=np.float32)
        # Tracks individual path points for trajectory traces
        self.active_paths: Dict[int, List[Tuple[int, int]]] = {}

    def update(self, active_tracks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Updates movement pathways and updates grid sector weights.
        Args:
            active_tracks: Output from tracker update.
        Returns:
            JSON-serializable metadata representation of the traffic density grid and paths.
        """
        # 1. Update active trajectory points
        current_ids = set()
        for track in active_tracks:
            person_id = track["person_id"]
            centroid = track["centroid"]
            is_occluded = track.get("occluded", False)
            
            if is_occluded:
                continue
                
            current_ids.add(person_id)
            
            # Map centroid to grid coordinate
            col = int((centroid[0] / self.width) * self.grid_cols)
            row = int((centroid[1] / self.height) * self.grid_rows)
            
            # Clamp grid index
            col = max(0, min(self.grid_cols - 1, col))
            row = max(0, min(self.grid_rows - 1, row))
            
            # Increment grid sector weight
            self.density_grid[row, col] += 1.0
            
            # Record track coordinate sequence
            if person_id not in self.active_paths:
                self.active_paths[person_id] = []
            self.active_paths[person_id].append(centroid)
            if len(self.active_paths[person_id]) > 100:
                self.active_paths[person_id].pop(0)

        # 2. Prune paths of shoppers who left the frame
        for pid in list(self.active_paths.keys()):
            if pid not in current_ids:
                del self.active_paths[pid]

        # 3. Classify traffic density zones
        max_density = float(np.max(self.density_grid))
        if max_density == 0:
            max_density = 1.0
            
        high_threshold = max_density * 0.65
        medium_threshold = max_density * 0.25

        high_traffic = []
        medium_traffic = []
        low_traffic = []

        # Pack grid weights and coordinates
        for r in range(self.grid_rows):
            for c in range(self.grid_cols):
                val = float(self.density_grid[r, c])
                if val == 0:
                    continue
                
                cell_data = {
                    "grid_coord": [c, r],
                    "weight": round(val, 2),
                    "normalized_weight": round(val / max_density, 3)
                }
                
                if val >= high_threshold:
                    high_traffic.append(cell_data)
                elif val >= medium_threshold:
                    medium_traffic.append(cell_data)
                else:
                    low_traffic.append(cell_data)

        # Return aggregated heatmap packets
        return {
            "grid_dimensions": [self.grid_cols, self.grid_rows],
            "max_val": float(max_density),
            "density_distribution": {
                "high": high_traffic,
                "medium": medium_traffic,
                "low": low_traffic
            },
            # Map paths to relative float coordinates [0.0 - 1.0] for responsive browser plotting
            "trajectories": {
                pid: [[round(pt[0]/self.width, 3), round(pt[1]/self.height, 3)] for pt in path]
                for pid, path in self.active_paths.items()
            }
        }
        
    def reset(self):
        """Clears accumulated density states."""
        self.density_grid.fill(0)
        self.active_paths.clear()
