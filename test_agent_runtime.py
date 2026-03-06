import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from Agent.runtime import AgentRuntime
from common.models import TelemetryEvent, CommandEvent, CommandAction, CommandActionPayload

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.agent_id = "test-agent"
    config.heartbeat_interval_sec = 0.1
    return config

@pytest.fixture
def mock_messaging():
    # We use AsyncMock because the MessageBroker methods are coroutines
    messaging = AsyncMock()
    messaging.connect = AsyncMock()
    messaging.publish = AsyncMock()
    messaging.subscribe = AsyncMock()
    return messaging

@pytest.fixture
def mock_metrics():
    metrics = MagicMock()
    metrics.collect.return_value = {
        "cpu_percent": 10.0,
        "memory_percent": 20.0,
        "disk_percent": 30.0,
        "active_processes": 5
    }
    return metrics

@pytest.fixture
def mock_executor():
    executor = AsyncMock()
    executor.execute.return_value = (MagicMock(value="SUCCESS"), {"output": "ok"})
    return executor

@pytest.mark.asyncio
async def test_agent_sampling_and_publishing(mock_config, mock_messaging, mock_metrics, mock_executor):
    """Test that the agent collects metrics and attempts to publish them."""
    runtime = AgentRuntime(mock_config, mock_messaging, mock_metrics, mock_executor)
    
    # Start the loops in the background
    task = asyncio.create_task(runtime.start())
    
    # Give it a moment to run a few cycles
    await asyncio.sleep(0.2)
    
    # Verify that metrics.collect was called
    assert mock_metrics.collect.called
    
    # Verify that messaging.publish was called with telemetry data
    mock_messaging.publish.assert_called()
    args, kwargs = mock_messaging.publish.call_args
    assert args[0] == "telemetry.v1"
    
    await runtime.stop()
    task.cancel()

@pytest.mark.asyncio
async def test_agent_command_handling(mock_config, mock_messaging, mock_metrics, mock_executor):
    """Test that the agent correctly handles an incoming command."""
    runtime = AgentRuntime(mock_config, mock_messaging, mock_metrics, mock_executor)
    
    # Create a dummy command
    cmd = CommandEvent(
        node_id="test-agent",
        epoch=1,
        action=CommandActionPayload(type=CommandAction.RESTART_PROCESS, target="sys")
    )
    
    # Manually trigger the internal handler
    await runtime._handle_command(cmd)
    
    # Verify executor was called
    mock_executor.execute.assert_called_once_with(cmd)
    # Verify result was published
    assert any(call.args[0] == "action_results.v1" for call in mock_messaging.publish.call_args_list)