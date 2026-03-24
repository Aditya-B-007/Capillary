import socket
import logging
import time

logger = logging.getLogger(__name__)

def register_agent(redis_client):
    """Registers the agent so the controller knows it exists."""
    # Use a unique agent_id (hostname is fine)
    agent_id = socket.gethostname()
    
    # Store agent in Redis using a structured key with a 10s TTL
    redis_key = f"agent:{agent_id}"
    redis_client.set(redis_key, "alive", ex=10)
    
    logger.info(f"Agent registered with key: {redis_key}")

def start_agent(redis_client):
    """Starts the main agent execution."""
    # Call register_agent() immediately after Redis connection is established
    register_agent(redis_client)
    
    try:
        while True:
            # Periodically renew the registration TTL
            redis_client.expire(f"agent:{socket.gethostname()}", 10)
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Agent shutting down.")