import asyncio
import numpy as np
from collections import deque, defaultdict
from sklearn.decomposition import IncrementalPCA # type: ignore
import eif # type: ignore

class FeatureExtractor:
    @staticmethod
    def cpu(m): return np.array([m.cpu_percent, m.load_1m, m.load_5m, m.load_15m, m.cpu_steal, m.cpu_user, m.cpu_system, m.context_switches, m.interrupts])
    @staticmethod
    def memory(m): return np.array([m.memory_percent, m.swap_usage, m.page_faults, m.memory_psi])
    @staticmethod
    def disk(m): return np.array([m.disk_percent, m.disk_iops, m.disk_latency, m.disk_util])
    @staticmethod
    def network(m): return np.array([m.net_in, m.net_out, m.packet_drops, m.tcp_retransmits])
    @staticmethod
    def os(m): return np.array([m.fd_usage, m.process_count, m.thread_count, m.zombie_processes, m.uptime])
    @staticmethod
    def hardware(m): return np.array([m.disk_health, m.temperature, m.fan_speed, m.power_usage])

class BayesianReasoner:
    def __init__(self):
        self.signal_counts = defaultdict(int)
        self.pair_counts = defaultdict(int)
        self.total = 0

    def update(self, signals: dict):
        active = [k for k, v in signals.items() if v]
        self.total += 1
        for s in active: self.signal_counts[s] += 1
        for i in range(len(active)):
            for j in range(i + 1, len(active)):
                key = tuple(sorted((active[i], active[j])))
                self.pair_counts[key] += 1

    def infer(self, signals: dict):
        active = [k for k, v in signals.items() if v]
        if not active: return ["No strong signals"]

        scores = []
        for s in active:
            base = self.signal_counts[s] / (self.total + 1e-6)
            boost = sum(self.pair_counts[tuple(sorted((s, other)))] / (self.total + 1e-6) for other in active if s != other)
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

def extract_signals(domain, rcf_score, eif_score):
    score = max(rcf_score, eif_score)
    return {
        "cpu_anomaly": domain == "cpu" and score > 3,
        "memory_pressure": domain == "memory" and score > 2.5,
        "disk_io_issue": domain == "disk" and score > 2.5,
        "network_instability": domain == "network" and score > 2.5,
        "resource_exhaustion": domain == "os" and score > 2.5,
        "hardware_issue": domain == "hardware" and score > 2.5,
    }

class HeavyDomainModel:
    def __init__(self, feature_dim, window_size=256):
        self.buffer = deque(maxlen=window_size)
        self.pca = IncrementalPCA(n_components=min(5, feature_dim))
        self.pca_ready = False
        self.eif_model = None
        self.last_eif_score = 0.0
        self.is_primed = False
        self._is_retraining = False
        self.step = 0

    def append_data(self, x):
        self.buffer.append(x)
        self.step += 1

class HeavyPredictorController:
    def __init__(self):
        self.domains = {}
        self.weights = {}
        self.feedback = {}
        self.reasoner = BayesianReasoner()
        self.event_queue = asyncio.Queue()
        self._register_domains()
        
        # Moved background workers safely away from the agent constraints
        asyncio.create_task(self._periodic_pca_eif_update())
        asyncio.create_task(self._event_consumer())

    def _register_domains(self):
        configs = {
            "cpu": (FeatureExtractor.cpu, 9), "memory": (FeatureExtractor.memory, 4),
            "disk": (FeatureExtractor.disk, 4), "network": (FeatureExtractor.network, 4),
            "os": (FeatureExtractor.os, 5), "hardware": (FeatureExtractor.hardware, 4)
        }
        for name, (extractor, dim) in configs.items():
            self.domains[name] = {"model": HeavyDomainModel(dim), "extractor": extractor}
            self.weights[name] = 1.0
            self.feedback[name] = {"tp": 1, "fp": 1}

    async def process_agent_payload(self, payload: dict):
        """Entrypoint for the controller. Call this when agent data comes via Message Broker"""
        metrics = payload["metrics"]
        is_anomaly = payload["anomaly"]
        domain_scores = payload["domain_scores"]
        for name, obj in self.domains.items():
            x = obj["extractor"](metrics)
            obj["model"].append_data(x)
        if is_anomaly:
            await self.event_queue.put((metrics, domain_scores))

    async def _periodic_pca_eif_update(self):
        """SLOW PATH: Batch learning execution"""
        while True:
            await asyncio.sleep(2.0)
            for name, obj in self.domains.items():
                model = obj["model"]
                if len(model.buffer) < 20: continue

                data = np.array(model.buffer)
                mean = np.mean(data, axis=0)
                std = np.std(data, axis=0) + 1e-6
                z = (data - mean) / std

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
            model.eif_model = await loop.run_in_executor(
                None, lambda: eif.iForest(z, ntrees=50, sample_size=len(z), ExtensionLevel=2)
            )
            model.is_primed = True
        finally:
            model._is_retraining = False

    async def _compute_eif_score(self, model, x):
        if not model.eif_model or not model.pca_ready:
            return 0.0
        data = np.array(model.buffer)
        z = (x - np.mean(data, axis=0)) / (np.std(data, axis=0) + 1e-6)
        z_pca = model.pca.transform(z.reshape(1, -1))[0]

        loop = asyncio.get_running_loop()
        score = await loop.run_in_executor(
            None, lambda: model.eif_model.compute_paths(X_in=z_pca.reshape(1, -1))[0]
        )
        return float(score)

    async def _event_consumer(self):
        """SLOW PATH: RCA pattern discovery"""
        while True:
            metrics, rcf_scores = await self.event_queue.get()
            signals = {}
            
            for name, obj in self.domains.items():
                model, extractor = obj["model"], obj["extractor"]
                x = extractor(metrics)
                
                eif_score = await self._compute_eif_score(model, x)
                model.last_eif_score = eif_score
                
                rcf = rcf_scores.get(name, 0.0)
                signals.update(extract_signals(name, rcf, eif_score))

            self.reasoner.update(signals)
            causes = self.reasoner.infer(signals)
            print(f"[CENTRAL BRAIN] Root Cause Analysis Result: {causes}")