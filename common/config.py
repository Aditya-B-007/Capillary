import os
import uuid
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class AgentConfig(BaseModel):
    agent_id: str
    broker_url: str
    heartbeat_interval_sec: float = 5.0

class ControllerConfig(BaseModel):
    controller_id: str
    broker_url: str
    web_port: int = 8000
    evaluation_interval_sec: float = 2.0
    node_timeout_sec: float = 15.0
    leader_lock_name: str = "capillary.leader.lock"

class Settings(BaseSettings):
    broker_url: str = os.getenv("BROKER_URL", "redis://localhost:6379")
    exchange_name: str = "capillary.events"
    service_name: str = "base-service"
    node_id: str = Field(default_factory=lambda: os.getenv("HOSTNAME") or f"agent-{uuid.uuid4().hex[:8]}")
    controller_id: str = "primary-controller"
    leader_lock_name: str = "capillary.leader.lock"
    log_level: str = "INFO"
    web_port: int = 8000
    heartbeat_interval_sec: float = 5.0
    evaluation_interval_sec: float = 2.0
    node_timeout_sec: float = 15.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )

settings = Settings()
