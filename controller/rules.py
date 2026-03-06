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
    def __init__(self, threshold_percent: float = 90.0):
        self.threshold_percent = threshold_percent

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
                payload={"target": "worker_process", "force": True},
                reason=f"Median memory usage ({median_mem:.1f}%) exceeded threshold ({self.threshold_percent}%)"
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
        ]
        logger.info(f"RuleEngine initialized with {len(self.rules)} rules.")

    def evaluate_cluster(self, state: ClusterState) -> List[ActionIntent]:
        intents: List[ActionIntent] = []
        all_nodes = state.get_all_nodes()

        for agent_id, node_record in all_nodes.items():
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