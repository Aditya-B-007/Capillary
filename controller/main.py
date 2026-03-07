import asyncio
import logging
import signal
import sys
from typing import Set, Dict, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from pydantic import BaseModel
import uvicorn
from common.config import settings, ControllerConfig
from common.messaging import create_messaging_client
from common.models import CommandAction
from controller.state import ClusterState
from controller.rules import RuleEngine, ActionIntent
from controller.dispatcher import CommandDispatcher
from controller.runtime import ControllerRuntime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("capillary.controller")

class ManualCommandRequest(BaseModel):
    node_id: str
    action: CommandAction
    parameters: Dict[str, Any] = {}

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
        web_port=settings.web_port,
        evaluation_interval_sec=settings.evaluation_interval_sec,
        node_timeout_sec=settings.node_timeout_sec,
        leader_lock_name=settings.leader_lock_name
    )
    
    logger.info(f"Starting Capillary Controller: {config.controller_id}")
    messaging_client = create_messaging_client(config.broker_url)
    cluster_state = ClusterState(timeout_sec=config.node_timeout_sec)
    dispatcher = CommandDispatcher(messaging_client=messaging_client)

    app = FastAPI(title="Capillary Controller API")

    @app.get("/api/v1/nodes")
    async def get_nodes():
        nodes_data = []
        for node_id, record in cluster_state.get_all_nodes().items():
            nodes_data.append({
                "node_id": node_id,
                "liveness": record.liveness,
                "metrics": {
                    "cpu_percent": record.cpu_window[-1] if record.cpu_window else None,
                    "memory_percent": record.mem_window[-1] if record.mem_window else None
                }
            })
        return nodes_data

    @app.post("/api/v1/nodes/{node_id}/restart")
    async def restart_node(node_id: str):
        intent = ActionIntent(
            agent_id=node_id,
            action=CommandAction.RESTART_PROCESS,
            payload={"target": "worker_process", "force": True},
            reason="Manual restart triggered via dashboard"
        )
        await dispatcher.dispatch(intent, cluster_state)
        return {"status": "sent", "node_id": node_id}

    @app.post("/api/v1/commands")
    async def manual_command(req: ManualCommandRequest):
        if not cluster_state.get_node(req.node_id):
            raise HTTPException(status_code=404, detail=f"Node {req.node_id} not found")
        
        if not cluster_state.can_remediate(req.node_id, req.action):
            raise HTTPException(status_code=429, detail=f"Action {req.action} is under cooldown for {req.node_id}")

        intent = ActionIntent(
            agent_id=req.node_id,
            action=req.action,
            payload=req.parameters,
            reason="Manual intervention via API"
        )
        await dispatcher.dispatch(intent, cluster_state)
        return {"status": "sent", "node_id": req.node_id, "action": req.action}

    rule_engine = RuleEngine()
    runtime = ControllerRuntime(
        config=config,
        messaging=messaging_client,
        state=cluster_state,
        rules=rule_engine,
        dispatcher=dispatcher
    )

    @app.websocket("/ws/v1/updates")
    async def websocket_updates(websocket: WebSocket):
        await websocket.accept()
        queue = asyncio.Queue(maxsize=100)
        runtime.register_ws_queue(queue)
        try:
            while True:
                data = await queue.get()
                await websocket.send_json(data)
        except WebSocketDisconnect:
            logger.info("WebSocket client disconnected")
        finally:
            runtime.unregister_ws_queue(queue)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig, 
            lambda s=sig: asyncio.create_task(shutdown(s.name, loop, runtime))
        )

    api_config = uvicorn.Config(app, host="0.0.0.0", port=config.web_port, log_level="info")
    api_server = uvicorn.Server(api_config)
    asyncio.create_task(api_server.serve())
        
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