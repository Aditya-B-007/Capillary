import abc
import logging
from typing import Callable, Awaitable, Optional
import redis.asyncio as redis
import asyncio

logger = logging.getLogger(__name__)

class MessageBroker(abc.ABC):
    @abc.abstractmethod
    async def connect(self) -> None:
        pass

    @abc.abstractmethod
    async def disconnect(self) -> None:
        pass

    @abc.abstractmethod
    async def publish(self, topic: str, payload: str) -> None:
        pass

    @abc.abstractmethod
    async def subscribe(self, topic: str, callback: Callable[[str], Awaitable[None]]) -> None:
        pass

    @abc.abstractmethod
    async def claim_leadership(self, lock_name: str) -> None:
        pass

class RedisMessagingClient(MessageBroker):
    def __init__(self, broker_url: str):
        self.broker_url = broker_url
        self._redis: Optional[redis.Redis] = None

    async def connect(self) -> None:
        logger.info(f"Connecting to Redis broker at {self.broker_url}")
        self._redis = redis.from_url(self.broker_url, decode_responses=True)
        await self._redis.ping() # type: ignore
        logger.info("Successfully connected to Redis.")

    async def disconnect(self) -> None:
        logger.info("Disconnecting from Redis...")
        if self._redis:
            await self._redis.aclose()
        logger.info("Redis disconnected safely.")

    async def publish(self, topic: str, payload: str) -> None:
        if not self._redis:
            raise RuntimeError("Cannot publish: Redis client is not connected.")
        await self._redis.publish(topic, payload)

    async def subscribe(self, topic: str, callback: Callable[[str], Awaitable[None]]) -> None:
        if not self._redis:
            raise RuntimeError("Cannot subscribe: Redis client is not connected.")
            
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(topic)

        async def message_handler():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        await callback(message["data"])
                    except Exception as e:
                        logger.error(f"Error in Redis callback for {topic}: {e}")
        
        asyncio.create_task(message_handler())
        logger.info(f"Subscribed to Redis topic: {topic}")

    async def claim_leadership(self, lock_name: str) -> None:
        if not self._redis:
            raise RuntimeError("Messaging client not connected. Call connect() first.")

        logger.info(f"Attempting to claim leadership via Redis lock: {lock_name}")
        while True:
            try:
                # Use SET with NX (Not Exists) and EX (Expire) for a distributed lock
                if await self._redis.set(lock_name, "leader", nx=True, ex=10):
                    logger.info("Leadership claimed. This node is now the ACTIVE leader.")
                    # Start background task to keep the lock alive
                    asyncio.create_task(self._refresh_leadership(lock_name))
                    return
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Unexpected error during leadership claim: {e}")
                await asyncio.sleep(5)

    async def _refresh_leadership(self, lock_name: str) -> None:
        """Background task to keep the leadership lock alive."""
        while self._redis:
            try:
                await asyncio.sleep(5)
                await self._redis.expire(lock_name, 10)
            except Exception:
                break

def create_messaging_client(broker_url: str) -> MessageBroker:
    if broker_url.startswith("redis://") or broker_url.startswith("rediss://"):
        return RedisMessagingClient(broker_url)
    
    raise ValueError(f"Unsupported broker URL scheme: {broker_url}")