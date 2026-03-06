import os
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

class AgentConfig(BaseModel):
    agent_id: str
    broker_url: str
    heartbeat_interval_sec: float = 5.0

class ControllerConfig(BaseModel):
    controller_id: str
    broker_url: str
    evaluation_interval_sec: float = 2.0
    node_timeout_sec: float = 15.0
    leader_lock_name: str = "capillary.leader.lock"

class Settings(BaseSettings):
    broker_url: str = "redis://localhost:6379/0"
    exchange_name: str = "capillary.events"
    service_name: str = "base-service"
    node_id: str = os.getenv("HOSTNAME", "unknown-node")
    controller_id: str = "primary-controller"
    leader_lock_name: str = "capillary.leader.lock"
    log_level: str = "INFO"
    heartbeat_interval_sec: float = 5.0
    evaluation_interval_sec: float = 2.0
    node_timeout_sec: float = 15.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False
    )

settings = Settings()