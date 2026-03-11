import asyncio
import logging
import numpy as np
import rrcf #type: ignore
import eif # type: ignore
from collections import deque
from typing import Literal
from common.models import Metrics

logger = logging.getLogger(__name__)

class EnsemblePredictor:
    def __init__(
        self, 
        window_size: int = 256, 
        num_trees: int = 100, 
        rcf_threshold: float = 3.5,
        eif_threshold: float = 0.75
    ):
        self.window_size = window_size
        self.num_trees = num_trees
        self.rcf_threshold = rcf_threshold
        self.eif_threshold = eif_threshold
        self.data_buffer = deque(maxlen=window_size)
        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        self.eif_model = None
        self.is_primed = False
        self._rolling_mean = np.zeros(4)
        self._rolling_std = np.ones(4)
        self._is_retraining = False
        self._step_counter = 0

    async def predict(self, metrics: Metrics) -> Literal["NORMAL", "ANOMALY", "LEARNING"]:
        features = np.array([
            metrics.cpu_percent, 
            metrics.memory_percent, 
            metrics.disk_percent, 
            float(metrics.active_processes)
        ])
        
        self.data_buffer.append(features)

        # Update rolling statistics to provide baseline context
        if len(self.data_buffer) > 1:
            snapshot = np.array(self.data_buffer)
            self._rolling_mean = np.mean(snapshot, axis=0)
            self._rolling_std = np.std(snapshot, axis=0) + 1e-6

        loop = asyncio.get_running_loop()
        self._step_counter += 1
        self.rcf.update(features)
        rcf_score = self.rcf.codisp() / self.num_trees if len(self.data_buffer) > 10 else 0
        
        eif_score = 0.0
        if len(self.data_buffer) >= self.window_size:
            if (not self.eif_model or self._step_counter % 500 == 0) and not self._is_retraining:
                asyncio.create_task(self._retrain_eif())
            model = self.eif_model
            if model:
                z_features = (features - self._rolling_mean) / self._rolling_std
                
                eif_score = await loop.run_in_executor(
                    None, 
                    lambda: model.compute_paths(X_in=z_features.reshape(1, -1))[0]
                )

        return self._evaluate_ensemble(rcf_score, eif_score)

    async def _retrain_eif(self):
        if self._is_retraining:
            return

        self._is_retraining = True
        try:
            data_matrix = np.array(self.data_buffer)
            mean = np.mean(data_matrix, axis=0)
            std = np.std(data_matrix, axis=0) + 1e-6
            z_matrix = (data_matrix - mean) / std

            loop = asyncio.get_running_loop()
            self.eif_model = await loop.run_in_executor(
                None, 
                lambda: eif.iForest(
                    z_matrix, 
                    ntrees=self.num_trees, 
                    sample_size=self.window_size, 
                    ExtensionLevel=3 
                )
            )
            self.is_primed = True
            logger.info("EIF model successfully updated in background.")
        except Exception as e:
            logger.error(f"EIF background training failed: {e}")
        finally:
            self._is_retraining = False

    def _evaluate_ensemble(self, rcf_score: float, eif_score: float) -> Literal["NORMAL", "ANOMALY", "LEARNING"]:
        if not self.is_primed:
            return "LEARNING"
        is_rcf_anomaly = rcf_score > self.rcf_threshold
        is_eif_anomaly = eif_score > self.eif_threshold

        if is_rcf_anomaly or is_eif_anomaly:
            logger.warning(
                f"Ensemble Anomaly Detected | RCF: {rcf_score:.2f} (>{self.rcf_threshold}) "
                f"| EIF: {eif_score:.3f} (>{self.eif_threshold})"
            )
            return "ANOMALY"

        return "NORMAL"
