import asyncio
import time
import logging
from typing import Any
from common.models import TelemetryEvent, ActionResultEvent
from controller.state import ClusterState
from controller.rules import RuleEngine, ActionIntent
from controller.dispatcher import CommandDispatcher

logger = logging.getLogger(__name__)


class ControllerRuntime:
    def __init__(
        self, 
        config: Any, 
        messaging: Any, 
        state: ClusterState, 
        rules: RuleEngine, 
        dispatcher: CommandDispatcher
    ):
        self.config = config
        self.messaging = messaging
        self.state = state
        self.rules = rules
        self.dispatcher = dispatcher
        
        self._stop_event = asyncio.Event()
        self._tasks = []

    async def start(self) -> None:
        logger.info("Starting Controller Runtime...")
        self._stop_event.clear()
        await self.messaging.connect()
        await self.messaging.subscribe("telemetry.v1", self._on_telemetry)
        await self.messaging.subscribe("action_results.v1", self._on_action_result)
        self._tasks.append(asyncio.create_task(self._reconciliation_loop()))
        
        logger.info("Controller Runtime fully operational.")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self) -> None:
        logger.info("Stopping Controller Runtime...")
        self._stop_event.set()
        
        for task in self._tasks:
            task.cancel()
            
        await self.messaging.disconnect()

    async def _on_telemetry(self, payload: str) -> None:
        try:
            event = TelemetryEvent.model_validate_json(payload)
            self.state.process_telemetry(event)
        except Exception as e:
            logger.error(f"Failed to process telemetry payload: {e}")

    async def _on_action_result(self, payload: str) -> None:
        try:
            result = ActionResultEvent.model_validate_json(payload)
            logger.info(
                f"Action Result from {result.node_id} "
                f"for command {result.command_id}: {result.status.value}"
            )
            if result.output: # type: ignore
                logger.debug(f"Command Output: {result.output}") # type: ignore
        except Exception as e:
            logger.error(f"Failed to process action result payload: {e}")

    async def _safe_dispatch(self, intent: ActionIntent) -> None:
        try:
            await self.dispatcher.dispatch(intent, self.state)
        except Exception as e:
            logger.error(f"Background dispatch failed for node {intent.agent_id}: {e}")

    async def _reconciliation_loop(self) -> None:
        logger.info(f"Reconciliation loop started (Interval: {self.config.evaluation_interval_sec}s).")
        
        while not self._stop_event.is_set():
            cycle_start = time.monotonic()
            
            try:
                self.state.evaluate_liveness()
                intents = self.rules.evaluate_cluster(self.state)
                for intent in intents:
                    asyncio.create_task(self._safe_dispatch(intent))
                    
            except Exception as e:
                logger.error(f"Error during reconciliation cycle: {e}", exc_info=True)
            elapsed = time.monotonic() - cycle_start
            sleep_duration = max(0, self.config.evaluation_interval_sec - elapsed)
            
            if elapsed > self.config.evaluation_interval_sec:
                logger.warning(f"Reconciliation cycle exceeded interval: {elapsed:.2f}s > {self.config.evaluation_interval_sec}s")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), 
                    timeout=sleep_duration
                )
            except asyncio.TimeoutError:
                pass 