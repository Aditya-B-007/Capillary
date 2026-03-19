import asyncio
import numpy as np
from collections import deque
from sklearn.decomposition import IncrementalPCA # type: ignore
import rrcf # type: ignore
import eif # type: ignore

class FeatureExtractor:
    @staticmethod
    def cpu(m):
        return np.array([
            m.cpu_percent,
            m.load_1m,
            m.load_5m,
            m.load_15m,
            m.cpu_steal,
            m.cpu_user,
            m.cpu_system,
            m.context_switches,
            m.interrupts
        ])

    @staticmethod
    def memory(m):
        return np.array([
            m.memory_percent,
            m.swap_usage,
            m.page_faults,
            m.memory_psi
        ])

    @staticmethod
    def disk(m):
        return np.array([
            m.disk_percent,
            m.disk_iops,
            m.disk_latency,
            m.disk_util
        ])

    @staticmethod
    def network(m):
        return np.array([
            m.net_in,
            m.net_out,
            m.packet_drops,
            m.tcp_retransmits
        ])

class DomainModel:
    def __init__(self, feature_dim, window_size=256, num_trees=50):
        self.buffer = deque(maxlen=window_size)
        self.mean = np.zeros(feature_dim)
        self.std = np.ones(feature_dim)

        self.pca = IncrementalPCA(n_components=min(5, feature_dim))
        self.is_pca_ready = False

        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        self.eif_model = None

        self._is_retraining = False
        self.is_primed = False
        self.step = 0

        # persistence tracking
        self.recent_flags = deque(maxlen=5)

    def update(self, x):
        self.buffer.append(x)
        self.step += 1

        if len(self.buffer) > 10:
            data = np.array(self.buffer)
            self.mean = np.mean(data, axis=0)
            self.std = np.std(data, axis=0) + 1e-6

        z = (x - self.mean) / self.std

        # Incremental PCA
        if len(self.buffer) >= self.pca.n_components_:
            self.pca.partial_fit(z.reshape(1, -1))
            self.is_pca_ready = True

        if self.is_pca_ready:
            z = self.pca.transform(z.reshape(1, -1))[0]

        # RCF update
        self.rcf.update(z)
        rcf_score = self.rcf.codisp() / 50 if len(self.buffer) > 10 else 0

        return z, rcf_score

    async def retrain_eif(self):
        if self._is_retraining or len(self.buffer) < 20:
            return

        self._is_retraining = True
        try:
            data = np.array(self.buffer)
            z = (data - self.mean) / self.std

            if self.is_pca_ready:
                z = self.pca.transform(z)

            loop = asyncio.get_running_loop()
            self.eif_model = await loop.run_in_executor(
                None,
                lambda: eif.iForest(
                    z,
                    ntrees=50,
                    sample_size=len(z),
                    ExtensionLevel=2
                )
            )

            self.is_primed = True
        finally:
            self._is_retraining = False

    async def score(self, z, rcf_score):
        eif_score = 0

        model = self.eif_model
        if model is not None:
            loop = asyncio.get_running_loop()
            eif_score = await loop.run_in_executor(
                None,
                lambda: model.compute_paths(
                    X_in=z.reshape(1, -1)
                )[0]
            )

        return rcf_score, eif_score

    def is_anomaly(self, rcf, eif, rcf_th=3.5, eif_th=0.75):
        flag = int(rcf > rcf_th or eif > eif_th)
        self.recent_flags.append(flag)

        # persistence logic
        return sum(self.recent_flags) >= 3

class MultiDomainPredictor:
    def __init__(self):
        self.domains = {
            "cpu": DomainModel(9),
            "memory": DomainModel(4),
            "disk": DomainModel(4),
            "network": DomainModel(4),
        }

        self.weights = {
            "cpu": 1,
            "memory": 2,
            "disk": 3,
            "network": 2
        }

        self.step = 0

    async def predict(self, metrics):
        self.step += 1
        total_score = 0

        for name, model in self.domains.items():
            extractor = getattr(FeatureExtractor, name)
            x = extractor(metrics)

            z, rcf = model.update(x)
            rcf, eif_score = await model.score(z, rcf)

            # periodic retrain
            if self.step % 200 == 0:
                asyncio.create_task(model.retrain_eif())

            if model.is_primed and model.is_anomaly(rcf, eif_score):
                total_score += self.weights[name]

        # decision
        if total_score >= 3:
            return "ANOMALY"

        if not all(m.is_primed for m in self.domains.values()):
            return "LEARNING"

        return "NORMAL"