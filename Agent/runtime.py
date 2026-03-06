import asyncio
import logging
from typing import Any

from common.models import (
    TelemetryEvent, 
    NodeStatus, 
    CommandEvent, 
    ActionResultEvent
)

logger = logging.getLogger(__name__)


class AgentRuntime:
    def __init__(self, config: Any, messaging: Any, metrics: Any, executor: Any):
        self.config = config
        self.messaging = messaging
        self.metrics = metrics
        self.executor = executor
        self._stop_event = asyncio.Event()
        self._telemetry_queue = asyncio.Queue(maxsize=100)
        self._tasks = []

    async def start(self):
        logger.info("Starting Agent Runtime loops...")
        self._stop_event.clear()
        await self.messaging.connect() #Connecting to Redis
        self._tasks.append(asyncio.create_task(self._sampling_loop()))
        self._tasks.append(asyncio.create_task(self._publisher_loop()))
        self._tasks.append(asyncio.create_task(self._command_listener()))
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self):
        logger.info("Stopping Agent Runtime...")
        self._stop_event.set()
        
        for task in self._tasks:
            task.cancel()
            
        await self.messaging.disconnect()

    async def _sampling_loop(self):
        logger.info("Sampling loop started.")
        interval = self.config.heartbeat_interval_sec
        loop = asyncio.get_running_loop()
        next_run = loop.time()
        
        while not self._stop_event.is_set():
            try:
                
                current_metrics = self.metrics.collect()
                event = TelemetryEvent(
                    node_id=self.config.agent_id,
                    status=NodeStatus.HEALTHY,
                    metrics=current_metrics
                )
                try:
                    self._telemetry_queue.put_nowait(event)
                except asyncio.QueueFull:
                    logger.warning("Telemetry queue full, dropping oldest sample.")
                    self._telemetry_queue.get_nowait()
                    self._telemetry_queue.put_nowait(event)
                
            except Exception as e:
                logger.error(f"Error in sampling loop: {e}")
            next_run += interval
            
            sleep_duration = max(0, next_run - loop.time())

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=sleep_duration)
            except asyncio.TimeoutError:
                pass  

    async def _publisher_loop(self):
        logger.info("Publisher loop started.")
        
        while not self._stop_event.is_set():
            try:
                try:
                    event = await asyncio.wait_for(self._telemetry_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue 
                try:
                    await asyncio.wait_for(
                        self.messaging.publish(topic="telemetry.v1", payload=event.model_dump_json()),
                        timeout=2.0
                    )
                    logger.debug(f"Published telemetry: {event.event_id}")
                finally:
                    self._telemetry_queue.task_done()
                
            except Exception as e:
                logger.error(f"Error in publisher loop: {e}")
                await asyncio.sleep(1.0)

    async def _command_listener(self):
        logger.info("Command listener started.")
        async def on_message(payload: str):
            try:
    
                cmd_event = CommandEvent.model_validate_json(payload)
                
                
                await self._handle_command(cmd_event)
                
            except Exception as e:
                logger.error(f"Failed to process incoming command payload: {e}")

        topic = f"commands.{self.config.agent_id}"
        await self.messaging.subscribe(topic, on_message)
        
        await self._stop_event.wait()

    async def _handle_command(self, cmd: CommandEvent):
        logger.info(f"Received command {cmd.command_id} [{cmd.action}]")
        
        result_status, output = await self.executor.execute(cmd)
        
        
        result_event = ActionResultEvent(
            command_id=cmd.command_id,
            node_id=self.config.agent_id,
            status=result_status,
            details=output)
    
        try:
            await self.messaging.publish(
                topic="action_results.v1",
                payload=result_event.model_dump_json()
            )
        except Exception as e:
            logger.error(f"Failed to publish command result for {cmd.command_id}: {e}")
            
        logger.info(f"Published result for command {cmd.command_id}: {result_status.value}")