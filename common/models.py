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
    swap_usage: float = Field(0.0, ge=0.0, le=100.0)
    page_faults: int = Field(0, ge=0) # System-wide page faults are hard to get directly from psutil
    memory_psi: float = Field(0.0, ge=0.0, le=100.0) # Pressure Stall Information (Linux-specific)
    disk_percent: float = Field(..., ge=0.0, le=100.0)
    disk_iops: int = Field(0, ge=0)
    disk_latency: float = Field(0.0, ge=0.0) # Hard to get generically
    disk_util: float = Field(0.0, ge=0.0, le=100.0) # Hard to get generically
    net_in: int = Field(0, ge=0)
    net_out: int = Field(0, ge=0)
    packet_drops: int = Field(0, ge=0) # Hard to get generically
    tcp_retransmits: int = Field(0, ge=0) # Hard to get generically
    load_1m: float = Field(0.0, ge=0.0)
    load_5m: float = Field(0.0, ge=0.0)
    load_15m: float = Field(0.0, ge=0.0)
    cpu_steal: float = Field(0.0, ge=0.0, le=100.0)
    cpu_user: float = Field(0.0, ge=0.0, le=100.0)
    cpu_system: float = Field(0.0, ge=0.0, le=100.0)
    context_switches: int = Field(0, ge=0)
    interrupts: int = Field(0, ge=0)
    fd_usage: float = Field(0.0, ge=0.0, le=100.0) # File descriptor usage (system-wide is hard)
    process_count: int = Field(..., ge=0) # Renamed from active_processes
    thread_count: int = Field(0, ge=0) # System-wide thread count is hard to get efficiently
    zombie_processes: int = Field(0, ge=0)
    uptime: float = Field(0.0, ge=0.0)
    disk_health: float = Field(0.0, ge=0.0, le=1.0) # Placeholder for SMART data etc.
    temperature: float = Field(0.0) # Requires specific sensors
    fan_speed: float = Field(0.0) # Requires specific sensors
    power_usage: float = Field(0.0) # Requires specific sensors


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
    is_anomaly: bool = False
    domain_scores: Dict[str, float] = Field(default_factory=dict)


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
