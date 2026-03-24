import asyncio
import logging
import sys
from Agent import discovery_listener
from Agent import agent_core

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Listening for controller discovery broadcast...")
    config = discovery_listener.listen()

    if not config or "redis_host" not in config:
        logger.error("Failed to discover Redis configuration.")
        sys.exit(1)
    redis_host = config["redis_host"]
    redis_port = config.get("redis_port", 6379)
    broker_url = f"redis://{redis_host}:{redis_port}/0"
    
    try:
        asyncio.run(agent_core.start_agent(broker_url))
    except KeyboardInterrupt:
        logger.info("Agent process terminated by user.")

if __name__ == "__main__":
    main()