import numpy as np
from collections import deque
import rrcf # type: ignore

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
        return np.array([m.memory_percent, m.swap_usage, m.page_faults, m.memory_psi])

    @staticmethod
    def disk(m):
        return np.array([m.disk_percent, m.disk_iops, m.disk_latency, m.disk_util])

    @staticmethod
    def network(m):
        return np.array([m.net_in, m.net_out, m.packet_drops, m.tcp_retransmits])

    @staticmethod
    def os(m):
        return np.array([m.fd_usage, m.process_count, m.thread_count, m.zombie_processes, m.uptime])

    @staticmethod
    def hardware(m):
        return np.array([m.disk_health, m.temperature, m.fan_speed, m.power_usage])


class DomainModelLight:
    def __init__(self, feature_dim: int, window_size: int = 256, num_trees: int = 50):
        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        self.recent_flags = deque(maxlen=5)
        self.num_trees = num_trees
        self.count = 0
        self.mean = np.zeros(feature_dim)
        self.m2 = np.zeros(feature_dim)
        self.std = np.ones(feature_dim)

    def update_fast(self, x: np.ndarray) -> tuple[np.ndarray, float]:
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

        if self.count > 1:
            variance = self.m2 / self.count
            self.std = np.sqrt(variance) + 1e-6

        z = (x - self.mean) / self.std
        self.rcf.update(z, self.count)

        rcf_score = 0.0
        if self.count > 10:
            rcf_score = self.rcf.codisp(self.count) / self.num_trees
        return z, rcf_score

    def is_anomaly(self, rcf_score, rcf_th=3.5):
        flag = int(rcf_score > rcf_th)
        self.recent_flags.append(flag)
        return sum(self.recent_flags) >= 3


class LightweightPredictor:
    def __init__(self):
        self.domains = {}
        self._register_domains()

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
            self.domains[name] = {"model": DomainModelLight(dim), "extractor": extractor}

    def predict(self, metrics):
        domain_scores = {}
        is_global_anomaly = False

        for name, obj in self.domains.items():
            x = obj["extractor"](metrics)
            _, rcf_score = obj["model"].update_fast(x)
            domain_scores[name] = rcf_score
            if obj["model"].is_anomaly(rcf_score):
                is_global_anomaly = True

        return {"metrics": metrics, "anomaly": is_global_anomaly, "domain_scores": domain_scores}