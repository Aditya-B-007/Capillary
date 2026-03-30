Capillary is a distributed system monitoring and control platform designed to run lightweight agents on multiple edge nodes. It aggregates system state, detects abnormal behavior using multi-tiered Machine Learning (Lightweight RCF on the edge, Heavyweight PCA/EIF on the controller), and executes safe corrective actions through a centralized controller.

Automatically detect and fix server issues before they crash.

## What it does
- Monitors system health in real-time
- Detects anomalies (CPU, memory, etc.)
- Automatically takes corrective actions (restart, cleanup)
- Stores incident reports for debugging

🌟 Key Features
 * Real-time Telemetry: Agents collect host-level CPU, memory, disk, and network metrics and stream them to the central controller.
 * Multi-Tier Anomaly Detection: * Edge (Agents): Uses Lightweight Random Cut Forests (RCF) for immediate local anomaly detection.
   * Central Brain (Controller): Uses Incremental Principal Component Analysis (PCA) and Extended Isolation Forests (EIF) for deep Root Cause Analysis (RCA).
 * Automated Remediation (Rule Engine): Automatically triggers actions like SCALE_DOWN, SCALE_UP, RESTART_PROCESS, or COLLECT_DIAGNOSTICS when configurable thresholds (e.g., High CPU, High Memory, Node Unresponsive) are breached.
 * Live Dashboard: A real-time web interface powered by WebSockets to monitor cluster health, node liveness, and recent automated/manual actions.
 * Resilient Architecture: Communicates entirely via asynchronous Pub/Sub messaging (Redis) with auto-discovery and TTL-based liveness tracking.

🏗 Architecture Components
 * Capillary Controller (/controller): The central brain of the cluster. It hosts the FastAPI web server, maintains cluster state, runs the heavy anomaly predictors, evaluates remediation rules, and dispatches commands.
 * Capillary Agent (/Agent): A lightweight daemon deployed on edge nodes. It auto-discovers the controller, publishes host-level metrics, and listens for remediation commands (executing them via local OS sub-processes).
 * Message Broker (Redis): Handles asynchronous pub/sub routing between the controller and agents.

📋 Prerequisites
To run the Capillary cluster, you must have the following installed on your host machine:
 * Docker
 * Docker Compose

🚀 Installation & Usage
1. Start the Cluster
To build the necessary images and launch the Redis broker, controller, and agent services simultaneously, execute the following command from the project root:
docker-compose up --build -d

Note: The -d flag runs the cluster in detached mode. Omit it if you want to see the live console logs.
2. Access the Dashboard
The Capillary Controller provides a real-time web-based dashboard. Once the containers are healthy, open your web browser and navigate to:
👉 http://localhost:8080
What you can view on the dashboard:
 * Cluster Nodes: A grid of all connected edge nodes, displaying their unique IDs.
 * Liveness Status: Color-coded status indicators (ONLINE, SUSPECT, UNRESPONSIVE).
 * Live Metrics: Real-time progress bars showing CPU and Memory utilization.
 * Action Log: A live feed of recent cluster events, automated rule triggers, and manual command results.
 * Manual Controls: Buttons to manually trigger process restarts on specific nodes.
3. Scaling the Environment
Capillary is built to scale dynamically. To simulate a larger distributed cluster, you can scale the number of monitoring agents on the fly:
docker-compose up --scale agent=3 -d

Each new agent container will automatically discover the Redis broker, register its TTL, and appear on the controller's dashboard within seconds.
4. Stop the System
To gracefully shut down the services, stop background tasks, and clean up the internal Docker network, run:
docker-compose down

⚙️ Configuration Variables
Capillary is highly configurable. You can adjust cluster behaviors by passing environment variables to the containers via docker-compose.yml or a .env file.
| Variable | Default Value | Description |
|---|---|---|
| BROKER_URL | redis://redis:6379/0 | The Redis connection string used for Pub/Sub messaging. |
| NODE_ID | Container Hostname | Unique identifier for the agent. Auto-generated if not specified. |
| WEB_PORT | 8000 | The internal port the FastAPI controller runs on (mapped to 8080 externally). |
| HEARTBEAT_INTERVAL_SEC | 5.0 | How often the edge agents push telemetry payloads to the controller. |
| EVALUATION_INTERVAL_SEC | 2.0 | How often the controller's Rule Engine evaluates cluster state for remediation. |
| NODE_TIMEOUT_SEC | 15.0 | Time before the controller marks an agent as UNRESPONSIVE. |


Rule Engine Thresholds
The automated remediation rules can be tuned inside common/config.py via the RuleConfig class:
 * high_mem_threshold: Default 92.0% (Triggers Process Restart)
 * high_cpu_threshold: Default 85.0% (Triggers Scale Down)
 * low_cpu_threshold: Default 30.0% (Triggers Scale Up / Recovery)
 * scale_down_factor: Default 0.5x


🌐 Distributed LAN Deployment

Capillary can monitor physical machines across your network:

1. Configure Controller IP: Update redis_host in controller/discovery_broadcaster.py with the Controller machine's actual LAN IP.
2. Run Controller (Machine A): docker-compose up redis controller
3. Run Agents (Machines B, C, etc.): docker-compose up agent
Note: Agents automatically use pid: "host" to monitor the actual hardware they reside on.

🛡️ License
MIT License
Copyright (c) 2026 Aditya-B-Dhruva
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
