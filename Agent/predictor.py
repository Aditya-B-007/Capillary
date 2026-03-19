import asyncio
import numpy as np
from collections import deque, defaultdict
from sklearn.decomposition import IncrementalPCA # type: ignore
import rrcf # type: ignore
import eif # type: ignore

class FeatureExtractor:
    @staticmethod
    def cpu(m):
        return np.array([
            m.cpu_percent, m.load_1m, m.load_5m, m.load_15m,
            m.cpu_steal, m.cpu_user, m.cpu_system,
            m.context_switches, m.interrupts
        ])

    @staticmethod
    def memory(m):
        return np.array([
            m.memory_percent, m.swap_usage,
            m.page_faults, m.memory_psi
        ])

    @staticmethod
    def disk(m):
        return np.array([
            m.disk_percent, m.disk_iops,
            m.disk_latency, m.disk_util
        ])

    @staticmethod
    def network(m):
        return np.array([
            m.net_in, m.net_out,
            m.packet_drops, m.tcp_retransmits
        ])

    @staticmethod
    def os(m):
        return np.array([
            m.fd_usage, m.process_count,
            m.thread_count, m.zombie_processes,
            m.uptime
        ])

    @staticmethod
    def hardware(m):
        return np.array([
            m.disk_health, m.temperature,
            m.fan_speed, m.power_usage
        ])

class DomainModel:
    def __init__(self, feature_dim, window_size=256, num_trees=50):
        self.buffer = deque(maxlen=window_size)
        self.mean = np.zeros(feature_dim)
        self.std = np.ones(feature_dim)
        self.pca = IncrementalPCA(n_components=min(5, feature_dim))
        self.pca_ready = False
        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        self.eif_model = None
        self.last_eif_score = 0.0
        self.is_primed = False
        self._is_retraining = False
        self.step = 0
        self.recent_flags = deque(maxlen=5)

    def update_fast(self, x):
        self.buffer.append(x)
        self.step += 1

        if len(self.buffer) > 10:
            data = np.array(self.buffer)
            self.mean = np.mean(data, axis=0)
            self.std = np.std(data, axis=0) + 1e-6

        z = (x - self.mean) / self.std
        if self.pca_ready:
            z = self.pca.transform(z.reshape(1, -1))[0]
        self.rcf.update(z)
        rcf_score = self.rcf.codisp() / 50 if len(self.buffer) > 10 else 0.0

        return z, rcf_score

    def is_anomaly(self, rcf_score, eif_score, rcf_th=3.5, eif_th=0.75):
        flag = int(rcf_score > rcf_th or eif_score > eif_th)
        self.recent_flags.append(flag)
        return sum(self.recent_flags) >= 3

class SlowPathWorker:
    def __init__(self, domains):
        self.domains = domains
        self.loop = asyncio.get_event_loop()

    async def periodic_update(self):
        while True:
            await asyncio.sleep(1.0)  # tune

            for name, obj in self.domains.items():
                model = obj["model"]

                if len(model.buffer) < 20:
                    continue

                data = np.array(model.buffer)
                z = (data - model.mean) / model.std
                try:
                    model.pca.partial_fit(z)
                    model.pca_ready = True
                except Exception:
                    pass
                if model.step % 200 == 0 and not model._is_retraining:
                    asyncio.create_task(self._retrain_eif(model, z))

    async def _retrain_eif(self, model, z):
        model._is_retraining = True
        try:
            loop = asyncio.get_running_loop()
            new_model = await loop.run_in_executor(
                None,
                lambda: eif.iForest(
                    z,
                    ntrees=50,
                    sample_size=len(z),
                    ExtensionLevel=2
                )
            )
            model.eif_model = new_model
            model.is_primed = True
        finally:
            model._is_retraining = False

    async def compute_eif_scores(self):
        while True:
            await asyncio.sleep(0.2)

            for obj in self.domains.values():
                model = obj["model"]

                if model.eif_model is None or len(model.buffer) == 0:
                    continue

                x = model.buffer[-1]
                z = (x - model.mean) / model.std

                if model.pca_ready:
                    z = model.pca.transform(z.reshape(1, -1))[0]

                loop = asyncio.get_running_loop()
                score = await loop.run_in_executor(
                    None,
                    lambda: model.eif_model.compute_paths(
                        X_in=z.reshape(1, -1)
                    )[0]
                )
                model.last_eif_score = float(score)

class BayesianReasoner:
    def __init__(self):
        self.signal_counts = defaultdict(int)
        self.pair_counts = defaultdict(int)
        self.total = 0

    def update(self, signals: dict):
        active = [k for k, v in signals.items() if v]
        self.total += 1

        for s in active:
            self.signal_counts[s] += 1

        for i in range(len(active)):
            for j in range(i + 1, len(active)):
                key = tuple(sorted((active[i], active[j])))
                self.pair_counts[key] += 1

    def infer(self, signals: dict):
        active = [k for k, v in signals.items() if v]
        if not active:
            return ["No strong signals"]

        scores = []

        for s in active:
            base = self.signal_counts[s] / (self.total + 1e-6)
            boost = 0.0
            for other in active:
                if s == other:
                    continue
                key = tuple(sorted((s, other)))
                boost += self.pair_counts[key] / (self.total + 1e-6)

            scores.append((s, base + boost))

        scores.sort(key=lambda x: x[1], reverse=True)
        mapping = {
            "cpu_anomaly": "CPU saturation or runaway process",
            "memory_pressure": "Memory pressure / possible leak",
            "disk_io_issue": "Disk bottleneck (latency/IOPS)",
            "network_instability": "Network packet loss / retransmits",
            "resource_exhaustion": "FD/process/thread exhaustion",
            "hardware_issue": "Thermal / hardware degradation"
        }

        return [mapping.get(s, s) for s, _ in scores[:2]]
    
def extract_signals(domain, rcf, eif):
    score = max(rcf, eif)

    return {
        "cpu_anomaly": domain == "cpu" and score > 3,
        "memory_pressure": domain == "memory" and score > 2.5,
        "disk_io_issue": domain == "disk" and score > 2.5,
        "network_instability": domain == "network" and score > 2.5,
        "resource_exhaustion": domain == "os" and score > 2.5,
        "hardware_issue": domain == "hardware" and score > 2.5,
    }

class MultiDomainPredictor:
    def __init__(self):
        self.domains = {}
        self.weights = {}
        self.feedback = {}
        self.reasoner = BayesianReasoner()
        self.event_queue = asyncio.Queue()
        self.step = 0
        self._register_domains()
        self.worker = SlowPathWorker(self.domains)
        asyncio.create_task(self.worker.periodic_update())
        asyncio.create_task(self.worker.compute_eif_scores())
        asyncio.create_task(self._event_consumer())

    def _register_domains(self):
        configs = {
            "cpu": (FeatureExtractor.cpu, 9),
            "memory": (FeatureExtractor.memory, 4),
            "disk": (FeatureExtractor.disk, 4),
            "network": (FeatureExtractor.network, 4),
            "os": (FeatureExtractor.os, 5),
            "hardware": (FeatureExtractor.hardware, 4),
        }

        for name, (extractor, dim) in configs.items():
            self.domains[name] = {
                "model": DomainModel(dim),
                "extractor": extractor
            }
            self.weights[name] = 1.0
            self.feedback[name] = {"tp": 1, "fp": 1}

    def _compute_weight(self, name):
        tp = self.feedback[name]["tp"]
        fp = self.feedback[name]["fp"]
        precision = tp / (tp + fp + 1e-6)
        return max(0.5, 0.5 + precision * 2.0)

    async def predict(self, metrics):
        """FAST PATH ONLY"""
        self.step += 1
        total_score = 0
        domain_votes = {}
        signals = {}

        for name, obj in self.domains.items():
            model = obj["model"]
            extractor = obj["extractor"]

            x = extractor(metrics)
            z, rcf = model.update_fast(x)

            eif_score = model.last_eif_score 

            is_anomaly = model.is_primed and model.is_anomaly(rcf, eif_score)
            domain_votes[name] = is_anomaly
            sig = extract_signals(name, rcf, eif_score)
            signals.update(sig)

            if is_anomaly:
                total_score += self._compute_weight(name)

        if total_score >= 3:
            await self.event_queue.put((metrics, signals))
            return {"status": "ANOMALY", "domains": domain_votes}

        if not all(obj["model"].is_primed for obj in self.domains.values()):
            return {"status": "LEARNING"}

        return {"status": "NORMAL"}

    async def _event_consumer(self):
        """SLOW PATH RCA"""
        while True:
            metrics, signals = await self.event_queue.get()
            self.reasoner.update(signals)
            causes = self.reasoner.infer(signals)
            print("RCA:", causes)

    def update_feedback(self, domain_votes, actual_label):
        for name, predicted in domain_votes.items():
            if predicted and actual_label == "ANOMALY":
                self.feedback[name]["tp"] += 1
            elif predicted and actual_label == "NORMAL":
                self.feedback[name]["fp"] += 1