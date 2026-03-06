import asyncio
import logging
import signal
import sys
from typing import Set
from common.config import settings, ControllerConfig
from common.messaging import create_messaging_client
from controller.state import ClusterState
from controller.rules import RuleEngine
from controller.dispatcher import CommandDispatcher
from controller.runtime import ControllerRuntime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("capillary.controller")

async def shutdown(signal_name: str, loop: asyncio.AbstractEventLoop, runtime: ControllerRuntime):
    logger.info(f"Received exit signal {signal_name}, initiating graceful shutdown...")
    await runtime.stop()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    logger.info(f"Cancelling {len(tasks)} outstanding background tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Controller shutdown complete.")

async def main():
    config = ControllerConfig(
        controller_id=settings.controller_id,
        broker_url=settings.broker_url,
        evaluation_interval_sec=settings.evaluation_interval_sec,
        node_timeout_sec=settings.node_timeout_sec,
        leader_lock_name=settings.leader_lock_name
    )
    
    logger.info(f"Starting Capillary Controller: {config.controller_id}")
    messaging_client = create_messaging_client(config.broker_url)
    cluster_state = ClusterState(timeout_sec=config.node_timeout_sec)
    dispatcher = CommandDispatcher(messaging_client=messaging_client)
    rule_engine = RuleEngine()
    runtime = ControllerRuntime(
        config=config,
        messaging=messaging_client,
        state=cluster_state,
        rules=rule_engine,
        dispatcher=dispatcher
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown(s.name, loop, runtime))
        )

    # Block here until this instance becomes the leader
    await messaging_client.connect()
    await messaging_client.claim_leadership(config.leader_lock_name)

    try:
        await runtime.start()
    except asyncio.CancelledError:
        logger.info("Controller main loop cancelled.")
    except Exception as e:
        logger.error(f"Fatal error in controller runtime: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Controller process terminated by user.")