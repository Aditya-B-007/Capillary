import asyncio
import logging
import redis.asyncio as redis
from common.messaging import create_messaging_client
from common.config import AgentConfig, settings
from Agent.metrics import MetricsCollector
from Agent.executor import CommandExecutor
from Agent.runtime import AgentRuntime

logger = logging.getLogger(__name__)

async def _maintain_registration(broker_url: str, agent_id: str):
    redis_client = None
    try:
        redis_client = redis.from_url(broker_url, decode_responses=True)
        redis_key = f"agent:{agent_id}"
        await redis_client.set(redis_key, "alive", ex=10)
        logger.info(f"Agent registered in Redis with key: {redis_key}")
        
        while True:
            await asyncio.sleep(5) # Non-blocking sleep
            await redis_client.expire(redis_key, 10)
            
    except asyncio.CancelledError:
        logger.info("Redis registration maintainer cancelled.")
    except Exception as e:
        logger.error(f"Error maintaining Redis registration: {e}")
    finally:
        if redis_client:
            await redis_client.aclose()

async def start_agent(broker_url: str):
    logger.info(f"Starting agent core connected to {broker_url}")
    agent_id = settings.node_id
    registration_task = asyncio.create_task(_maintain_registration(broker_url, agent_id))
    messaging = create_messaging_client(broker_url)
    agent_config = AgentConfig(
        agent_id=agent_id, 
        broker_url=broker_url,
        heartbeat_interval_sec=settings.heartbeat_interval_sec
    )
    
    metrics = MetricsCollector()
    executor = CommandExecutor()
    runtime = AgentRuntime(
        config=agent_config, 
        messaging=messaging, 
        metrics=metrics, 
        executor=executor
    )
    
    try:
        await runtime.start()
    except asyncio.CancelledError:
        logger.info("Agent core cancelled, initiating shutdown...")
        await runtime.stop()
    except Exception as e:
        logger.error(f"Fatal error in agent core: {e}", exc_info=True)
        await runtime.stop()
    finally:
        registration_task.cancel()
        await asyncio.gather(registration_task, return_exceptions=True)