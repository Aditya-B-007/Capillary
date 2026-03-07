import uuid
from time import time
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, ConfigDict, Field


class MessageType(str, Enum): #This is for Redis (message broker)
    TELEMETRY = "TELEMETRY"
    HEARTBEAT = "HEARTBEAT"
    COMMAND = "COMMAND"
    ACTION_RESULT = "ACTION_RESULT"


class NodeStatus(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    SUSPECT= "SUSPECT"
    UNRESPONSIVE = "UNRESPONSIVE"
    UNKNOWN = "UNKNOWN"
    RECOVERING = "RECOVERING"
    FAILED = "FAILED"
    

class CommandAction(str, Enum):
    RESTART_PROCESS = "RESTART_PROCESS"
    COLLECT_DIAGNOSTICS = "COLLECT_DIAGNOSTICS"
    THROTTLE_WORKLOAD = "THROTTLE_WORKLOAD"
    SCALE_DOWN = "SCALE_DOWN"
    SCALE_UP = "SCALE_UP"


class ActionResultStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    REJECTED_STALE = "REJECTED_STALE"  


class Metrics(BaseModel):
    cpu_percent: float = Field(..., ge=0.0, le=100.0)
    memory_percent: float = Field(..., ge=0.0, le=100.0)
    disk_percent: float = Field(..., ge=0.0, le=100.0)
    active_processes: int = Field(..., ge=0)


class ProcessInfo(BaseModel):
    pid: int
    rss_bytes: int
    healthy: bool


class HeartbeatEvent(BaseModel):
    model_config = ConfigDict(frozen=True) 
    schema_version: int = 1
    message_type: MessageType = MessageType.HEARTBEAT
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None
    node_id: str = Field(..., description="Unique identifier for the edge node")
    timestamp: int = Field(default_factory=lambda: int(time()))
    status: NodeStatus = NodeStatus.HEALTHY


class TelemetryEvent(HeartbeatEvent):
    message_type: MessageType = MessageType.TELEMETRY
    metrics: Metrics
    process_state: Dict[str, ProcessInfo] = Field(default_factory=dict)


class CommandActionPayload(BaseModel):
    type: CommandAction
    target: str
    parameters: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Optional extra arguments for the action"
    )


class CommandEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: int = 1
    message_type: MessageType = MessageType.COMMAND
    command_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()), 
        description="Globally unique ID to enforce idempotency at the agent level"
    )
    correlation_id: Optional[str] = None
    node_id: str
    epoch: int = Field(
        ..., 
        description="Fencing token from the controller. Agents reject commands with older epochs."
    )
    action: CommandActionPayload
    timestamp: int = Field(default_factory=lambda: int(time()))


class ActionResultEvent(BaseModel):
    model_config = ConfigDict(frozen=True)
    schema_version: int = 1
    message_type: MessageType = MessageType.ACTION_RESULT
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: Optional[str] = None
    command_id: str = Field(..., description="Correlates directly to the triggering CommandEvent")
    node_id: str
    status: ActionResultStatus
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: int = Field(default_factory=lambda: int(time()))