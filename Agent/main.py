import asyncio
import logging
import signal
import sys
from typing import Set
from common.config import settings, AgentConfig
from common.messaging import create_messaging_client  # Factory function for transport
from metrics import MetricsCollector
from executor import CommandExecutor
from runtime import AgentRuntime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("capillary.agent")

async def shutdown(signal_name: str, loop: asyncio.AbstractEventLoop, runtime: AgentRuntime):
    logger.info(f"Received exit signal {signal_name}...")
    await runtime.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Agent shutdown complete.")

async def main():
    config = AgentConfig(
        agent_id=settings.node_id, 
        broker_url=settings.broker_url,
        heartbeat_interval_sec=settings.heartbeat_interval_sec
    )
    
    logger.info(f"Starting Capillary Agent: {config.agent_id}")
    messaging_client = create_messaging_client(config.broker_url)
    metrics_collector = MetricsCollector()
    executor = CommandExecutor()
    runtime = AgentRuntime(
        config=config,
        messaging=messaging_client,
        metrics=metrics_collector,
        executor=executor
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown(s.name, loop, runtime))
        )
    try:
        await runtime.start()
    except asyncio.CancelledError:
        logger.info("Main run loop cancelled.")
    except Exception as e:
        logger.error(f"Fatal error in agent runtime: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Agent process terminated by user.")
