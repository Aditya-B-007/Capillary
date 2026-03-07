import logging
import statistics
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from common.models import CommandAction
from controller.state import ClusterState, NodeRecord, LivenessState

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ActionIntent:
    agent_id: str
    action: CommandAction
    payload: Dict[str, Any]
    reason: str


class Rule(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for the rule."""
        pass

    @abstractmethod
    def evaluate(self, node: NodeRecord) -> Optional[ActionIntent]:
        pass


class HighMemoryRule(Rule):    
    def __init__(self, threshold_percent: float = 90.0, target_process: str = "worker_process"):
        self.threshold_percent = threshold_percent
        self.target_process = target_process

    @property
    def name(self) -> str:
        return "HighMemoryRule"

    def evaluate(self, node: NodeRecord) -> Optional[ActionIntent]:
        if node.liveness != LivenessState.ONLINE or not node.mem_window:
            return None
        if node.mem_window.maxlen is None or len(node.mem_window) < node.mem_window.maxlen:
            return None

        median_mem = statistics.median(node.mem_window)
        
        if median_mem >= self.threshold_percent:
            return ActionIntent(
                agent_id=node.node_id,
                action=CommandAction.RESTART_PROCESS,
                payload={"target": self.target_process, "force": True},
                reason=f"Median memory usage ({median_mem:.1f}%) exceeded threshold ({self.threshold_percent}%)"
            )
        return None


class HighCPURule(Rule):
    def __init__(
        self, 
        threshold_percent: float = 80.0, 
        target_process: str = "worker_process",
        scale_factor: float = 0.5
    ):
        self.threshold_percent = threshold_percent
        self.target_process = target_process
        self.scale_factor = scale_factor

    @property
    def name(self) -> str:
        return "HighCPURule"

    def evaluate(self, node: NodeRecord) -> Optional[ActionIntent]:
        if node.liveness != LivenessState.ONLINE or not node.cpu_window:
            return None
        if node.cpu_window.maxlen is None or len(node.cpu_window) < node.cpu_window.maxlen:
            return None
        valid_samples = [s for s in node.cpu_window if s is not None]
        if not valid_samples:
            return None
            
        median_cpu = statistics.median(valid_samples)

        if median_cpu >= self.threshold_percent:
            return ActionIntent(
                agent_id=node.node_id,
                action=CommandAction.SCALE_DOWN,
                payload={"target": self.target_process, "factor": self.scale_factor},
                reason=f"Median CPU usage ({median_cpu:.1f}%) exceeded threshold ({self.threshold_percent}%)"
            )
        return None


class LowCPURule(Rule):
    def __init__(self, threshold_percent: float = 30.0, target_process: str = "worker_process"):
        self.threshold_percent = threshold_percent
        self.target_process = target_process

    @property
    def name(self) -> str:
        return "LowCPURule"

    def evaluate(self, node: NodeRecord) -> Optional[ActionIntent]:
        if node.liveness != LivenessState.ONLINE or not node.cpu_window:
            return None
        if node.cpu_window.maxlen is None or len(node.cpu_window) < node.cpu_window.maxlen:
            return None

        if getattr(node, "current_scale_factor", 1.0) >= 1.0:
            return None

        valid_samples = [s for s in node.cpu_window if s is not None]
        if not valid_samples:
            return None
            
        median_cpu = statistics.median(valid_samples)

        if median_cpu < self.threshold_percent:
            return ActionIntent(
                agent_id=node.node_id,
                action=CommandAction.SCALE_UP,
                payload={"target": self.target_process},
                reason=f"Median CPU usage ({median_cpu:.1f}%) is below recovery threshold ({self.threshold_percent}%)."
            )
        return None


class UnresponsiveNodeRule(Rule):
    @property
    def name(self) -> str:
        return "UnresponsiveNodeRule"

    def evaluate(self, node: NodeRecord) -> Optional[ActionIntent]:
        if node.liveness == LivenessState.UNRESPONSIVE:
            return ActionIntent(
                agent_id=node.node_id,
                action=CommandAction.COLLECT_DIAGNOSTICS,
                payload={"level": "deep", "include_logs": True},
                reason="Node stopped sending telemetry heartbeats."
            )
        return None


class RuleEngine:
    def __init__(self, rules: Optional[List[Rule]] = None):
        self.rules = rules or [
            UnresponsiveNodeRule(),
            HighMemoryRule(threshold_percent=92.0),
            HighCPURule(threshold_percent=85.0),
            LowCPURule(threshold_percent=30.0),
        ]
        logger.info(f"RuleEngine initialized with {len(self.rules)} rules.")

    def evaluate_cluster(self, state: ClusterState) -> List[ActionIntent]:
        intents: List[ActionIntent] = []

        for agent_id, node_record in state.iter_nodes():
            for rule in self.rules:
                try:
                    intent = rule.evaluate(node_record)
                    if intent:
                        if not state.can_remediate(agent_id, intent.action):
                            continue

                        logger.info(f"Rule {rule.name} triggered for {agent_id}: {intent.reason}")
                        intents.append(intent)
                        break 
                        
                except Exception as e:
                    logger.error(f"Error evaluating rule {rule.name} on agent {agent_id}: {e}")

        if intents:
            logger.info(f"RuleEngine generated {len(intents)} action intents.")
            
        return intents