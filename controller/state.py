import logging
import time
from typing import Dict, Optional
from dataclasses import dataclass, field
from collections import deque
from common.models import TelemetryEvent, CommandAction

logger = logging.getLogger(__name__)


class LivenessState(str):
    ONLINE = "ONLINE"
    SUSPECT = "SUSPECT"
    UNRESPONSIVE = "UNRESPONSIVE"


@dataclass
class NodeRecord:
    node_id: str
    last_telemetry: Optional[TelemetryEvent] = None
    last_seen_monotonic: float = field(default_factory=time.monotonic)
    liveness: str = LivenessState.ONLINE
    cpu_window: deque = field(default_factory=lambda: deque(maxlen=5))
    mem_window: deque = field(default_factory=lambda: deque(maxlen=5))
    last_command_at: Optional[float] = None
    action_cooldowns: Dict[CommandAction, float] = field(default_factory=dict)

class ClusterState:
    def __init__(self, timeout_sec: float, memory_window_size: int = 5):
        self.timeout_sec = timeout_sec
        self.suspect_sec = timeout_sec / 2.0
        self.memory_window_size = memory_window_size
        self._nodes: Dict[str, NodeRecord] = {}

    def process_telemetry(self, event: TelemetryEvent) -> None:
        node_id = event.node_id

        if node_id not in self._nodes:
            logger.info(f"Discovered new node: {node_id}")
            self._nodes[node_id] = NodeRecord(
                node_id=node_id,
                cpu_window=deque(maxlen=self.memory_window_size),
                mem_window=deque(maxlen=self.memory_window_size)
            )

        record = self._nodes[node_id]
        record.last_telemetry = event
        record.last_seen_monotonic = time.monotonic()
        record.liveness = LivenessState.ONLINE
        record.cpu_window.append(event.metrics.cpu_percent)
        record.mem_window.append(event.metrics.memory_percent)

    def evaluate_liveness(self) -> None:
        now = time.monotonic()

        for node_id, record in self._nodes.items():
            elapsed = now - record.last_seen_monotonic

            if elapsed >= self.timeout_sec:
                if record.liveness != LivenessState.UNRESPONSIVE:
                    logger.warning(f"{node_id} timed out ({elapsed:.1f}s). Marking UNRESPONSIVE.")
                record.liveness = LivenessState.UNRESPONSIVE

            elif elapsed >= self.suspect_sec:
                if record.liveness == LivenessState.ONLINE:
                    logger.debug(f"{node_id} delayed ({elapsed:.1f}s). Marking SUSPECT.")
                record.liveness = LivenessState.SUSPECT

            else:
                record.liveness = LivenessState.ONLINE

    def can_remediate(self, node_id: str, action: CommandAction) -> bool:
        record = self._nodes.get(node_id)
        if not record:
            return False

        now = time.monotonic()
        cooldown_until = record.action_cooldowns.get(action, 0)
        
        if now < cooldown_until:
            return False

        return True

    def record_remediation(self, node_id: str, action: CommandAction, cooldown_sec: float) -> None:
        record = self._nodes.get(node_id)
        if not record:
            return

        now = time.monotonic()
        record.last_command_at = now
        record.action_cooldowns[action] = now + cooldown_sec

    def get_node(self, node_id: str) -> Optional[NodeRecord]:
        return self._nodes.get(node_id)

    def get_all_nodes(self) -> Dict[str, NodeRecord]:
        return self._nodes.copy()