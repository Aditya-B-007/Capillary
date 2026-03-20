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
    def __init__(self, feature_dim, window_size=256, num_trees=50):
        self.buffer = deque(maxlen=window_size)
        self.mean = np.zeros(feature_dim)
        self.std = np.ones(feature_dim)
        self.rcf = rrcf.RCForest(num_trees=num_trees, tree_size=window_size)
        self.recent_flags = deque(maxlen=5)

    def update_fast(self, x):
        self.buffer.append(x)
        if len(self.buffer) > 10:
            data = np.array(self.buffer)
            self.mean = np.mean(data, axis=0)
            self.std = np.std(data, axis=0) + 1e-6
        z = (x - self.mean) / self.std
        self.rcf.update(z)
        rcf_score = self.rcf.codisp() / 50 if len(self.buffer) > 10 else 0.0

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