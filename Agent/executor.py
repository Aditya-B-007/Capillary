import asyncio
import json
import logging
import os
from typing import Tuple, Set
from common.models import CommandEvent, CommandAction, ActionResultStatus

logger = logging.getLogger(__name__)


class CommandExecutor:
    def __init__(self, state_file: str = "agent_state.json"):
        self.state_file = state_file
        self._executed_commands: Set[str] = set()
        self._highest_seen_epoch: int = 0
        self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    self._highest_seen_epoch = data.get("highest_seen_epoch", 0)
                    self._executed_commands = set(data.get("executed_commands", []))
                logger.info(f"Loaded state from {self.state_file}: epoch={self._highest_seen_epoch}")
            except Exception as e:
                logger.error(f"Failed to load state from {self.state_file}: {e}")

    def _save_state(self):
        try:
            temp_file = f"{self.state_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump({
                    "highest_seen_epoch": self._highest_seen_epoch,
                    "executed_commands": list(self._executed_commands)
                }, f)
            os.replace(temp_file, self.state_file)
        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file}: {e}")

    async def execute(self, cmd: CommandEvent) -> Tuple[ActionResultStatus, str]:
        if cmd.epoch < self._highest_seen_epoch:
            logger.warning(
                f"Rejecting stale command {cmd.command_id}. "
                f"Command Epoch: {cmd.epoch}, "
                f"Agent Highest Epoch: {self._highest_seen_epoch}."
            )
            return ActionResultStatus.REJECTED_STALE, "Epoch too low."

        if cmd.epoch > self._highest_seen_epoch:
            logger.info(f"New epoch {cmd.epoch} detected. Resetting command history.")
            self._executed_commands.clear()
            self._highest_seen_epoch = cmd.epoch
            self._save_state()

        if cmd.command_id in self._executed_commands:
            logger.info(f"Ignoring duplicate command {cmd.command_id}.")
            return ActionResultStatus.SUCCESS, "Already executed (Idempotent success)."
            
        self._executed_commands.add(cmd.command_id)
        self._save_state()
        try:
            action_params = {**cmd.action.parameters, "target": cmd.action.target}

            if cmd.action.type == CommandAction.RESTART_PROCESS:
                return await self._action_restart_process(action_params)
                
            elif cmd.action.type == CommandAction.COLLECT_DIAGNOSTICS:
                return await self._action_collect_diagnostics(action_params)
                
            else:
                logger.error(f"Unknown command action: {cmd.action.type}")
                return ActionResultStatus.FAILED, f"Unsupported action: {cmd.action.type}"
                
        except Exception as e:
            logger.error(f"Execution failed for {cmd.command_id}: {e}", exc_info=True)
            return ActionResultStatus.FAILED, str(e)

    async def _action_restart_process(self, payload: dict) -> Tuple[ActionResultStatus, str]:
        target = payload.get("target")
        if not target:
            return ActionResultStatus.FAILED, "Missing 'target' in payload."
            
        logger.info(f"Executing OS Command: systemctl restart {target}")
        process = await asyncio.create_subprocess_exec(
            'systemctl', 'restart', target,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15.0)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            logger.error(f"Timeout restarting process {target}")
            return ActionResultStatus.FAILED, f"Restart command for {target} timed out."

        if process.returncode == 0:
            return ActionResultStatus.SUCCESS, f"Process {target} restarted successfully."
        else:
            error_msg = stderr.decode().strip() or stdout.decode().strip()
            return ActionResultStatus.FAILED, f"Failed to restart {target}: {error_msg}"

    async def _action_collect_diagnostics(self, payload: dict) -> Tuple[ActionResultStatus, str]:
        level = payload.get("level", "standard")
        logger.info(f"Executing OS Command: Gathering {level} diagnostics...")
        
        try:
            p_uptime = await asyncio.create_subprocess_exec('uptime', stdout=asyncio.subprocess.PIPE)
            p_free = await asyncio.create_subprocess_exec('free', '-m', stdout=asyncio.subprocess.PIPE)
            p_logs = await asyncio.create_subprocess_exec('journalctl', '-n', '20', '--no-pager', stdout=asyncio.subprocess.PIPE)

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(p_uptime.communicate(), p_free.communicate(), p_logs.communicate()),
                    timeout=10.0
                )
                (out_uptime, _), (out_free, _), (out_logs, _) = results
            except asyncio.TimeoutError:
                for p in [p_uptime, p_free, p_logs]:
                    try:
                        p.kill()
                        await p.wait()
                    except Exception: pass
                return ActionResultStatus.FAILED, "Diagnostic collection timed out."

            output = (
                f"Diagnostic report ({level}):\n"
                f"- Load Average: {out_uptime.decode().strip()}\n"
                f"- Memory/Swap Usage:\n{out_free.decode().strip()}\n"
                f"- Recent Logs:\n{out_logs.decode().strip()}"
            )
            return ActionResultStatus.SUCCESS, output
        except Exception as e:
            return ActionResultStatus.FAILED, f"Failed to collect diagnostics: {str(e)}"