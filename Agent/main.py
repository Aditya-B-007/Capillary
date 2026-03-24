import logging
import sys
import discovery_listener
import redis_client
import agent_core
from Agent import discovery_listener
from common.messaging import create_messaging_client
from Agent import agent_core

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    # Call discovery_listener.listen() exactly once during startup
    logger.info("Listening for controller discovery broadcast...")
    config = discovery_listener.listen()

    if not config or "redis_host" not in config:
        logger.error("Failed to discover Redis configuration.")
        sys.exit(1)

    # Use returned config to initialize Redis using redis_client.connect_to_redis()
    redis_host = config["redis_host"]
    redis_port = config.get("redis_port", 6379)
    r_client = redis_client.connect_to_redis(redis_host, redis_port)

    # Pass the Redis client into agent_core
    logger.info("Starting agent core...")
    agent_core.start_agent(r_client)

if __name__ == "__main__":
    main()
