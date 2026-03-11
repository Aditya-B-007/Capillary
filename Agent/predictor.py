import logging
import numpy as np
import rrcf
import eif
from collections import deque
from typing import Literal
from common.models import Metrics

logger = logging.getLogger(__name__)

class EnsemblePredictor:
    def __init__(
        self, 
        window_size: int = 256, 
        num_trees: int = 100, 
        rcf_threshold: float = 64.0,
        eif_threshold: float = 0.70
    ):
        self.window_size = window_size
        self.num_trees = num_trees
        self.rcf_threshold = rcf_threshold
        self.eif_threshold = eif_threshold
        
        # Buffer for EIF batch training and multivariate context
        self.data_buffer = deque(maxlen=window_size)
        
        # Initialize Random Cut Forest for streaming displacement detection
        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        
        # EIF model is trained once the buffer is primed
        self.eif_model = None
        self.is_primed = False
        self._step_counter = 0

    def predict(self, metrics: Metrics) -> Literal["NORMAL", "ANOMALY", "LEARNING"]:
        """
        Processes new metrics and returns the ensemble health classification.
        """
        # 1. Feature Extraction
        # We capture CPU, Memory, Disk, and Process Count as a single vector
        point = np.array([
            metrics.cpu_percent, 
            metrics.memory_percent, 
            metrics.disk_percent, 
            float(metrics.active_processes)
        ])
        
        self.data_buffer.append(point)
        self._step_counter += 1

        # 2. Random Cut Forest (Streaming Score)
        # RCF measures "Collusive Displacement" (how much this point changes the model)
        self.rcf.update(point)
        rcf_score = self.rcf.codisp() / self.num_trees if len(self.data_buffer) > 10 else 0

        # 3. Extended Isolation Forest (Structural Score)
        eif_score = 0.0
        if len(self.data_buffer) >= self.window_size:
            # Periodically retrain EIF to adapt to local baseline drift
            if not self.eif_model or self._step_counter % 100 == 0:
                data_matrix = np.array(list(self.data_buffer))
                # ExtensionLevel=3 allows for hyperplanes with random slopes
                self.eif_model = eif.iForest(
                    data_matrix, 
                    ntrees=self.num_trees, 
                    sample_size=self.window_size, 
                    ExtensionLevel=3 
                )
                self.is_primed = True
            
            # Compute anomaly score (closer to 1.0 is more anomalous)
            eif_score = self.eif_model.compute_paths(X_in=point.reshape(1, -1))[0]

        return self._evaluate_ensemble(rcf_score, eif_score)

    def _evaluate_ensemble(self, rcf_s: float, eif_s: float) -> str:
        """
        Combines scores from both models into a single health status.
        """
        if not self.is_primed:
            return "LEARNING"

        # Logic: Trigger if the streaming spike is massive (RCF) 
        # OR if the multivariate relationship is structurally broken (EIF)
        is_rcf_anomaly = rcf_s > self.rcf_threshold
        is_eif_anomaly = eif_s > self.eif_threshold

        if is_rcf_anomaly or is_eif_anomaly:
            logger.warning(
                f"Ensemble Anomaly Detected | RCF: {rcf_s:.2f} (>{self.rcf_threshold}) "
                f"| EIF: {eif_s:.3f} (>{self.eif_threshold})"
            )
            return "ANOMALY"

        return "NORMAL"