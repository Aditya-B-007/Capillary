import logging
from common.models import CommandEvent, CommandActionPayload
from common.messaging import MessageBroker
from controller.rules import ActionIntent
from controller.state import ClusterState
import uuid
logger = logging.getLogger(__name__)

class CommandDispatcher:
    def __init__(self, messaging_client: MessageBroker, cooldown_sec: float = 30.0):
        self.messaging = messaging_client
        self.cooldown_sec = cooldown_sec
        self.current_epoch = int(uuid.uuid4().hex)

    async def dispatch(self, intent: ActionIntent, state: ClusterState) -> None:
        agent_id = intent.agent_id
        payload_params = intent.payload.copy()
        target = payload_params.pop("target", agent_id)
        command = CommandEvent(
            node_id=agent_id,
            epoch=self.current_epoch,
            action=CommandActionPayload(
                type=intent.action,
                target=target,
                parameters=payload_params
            )
        )
        topic = f"commands.{agent_id}"
        try:
            await self.messaging.publish(topic, command.model_dump_json())
            state.record_remediation(agent_id, intent.action, self.cooldown_sec)
            
            logger.info(
                f"Dispatched {command.action.type.value} to {agent_id} "
                f"(ID: {command.command_id}, Reason: {intent.reason})"
            )
            
        except Exception as e:
            logger.error(f"Failed to dispatch command to {agent_id}: {e}")