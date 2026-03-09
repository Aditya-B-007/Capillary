# Capillary
a distributed system monitoring and control platform that runs lightweight agents on multiple nodes, aggregates system state, detects abnormal behavior, and executes safe corrective actions through a centralized or elected controller. 

<img width="1050" height="724" alt="image" src="https://github.com/user-attachments/assets/15dc5c5c-8b82-495c-bc8f-e0d7f9e89976" />

<img width="679" height="883" alt="image" src="https://github.com/user-attachments/assets/7e1e1cca-6652-4f3f-8b8d-f97559f13291" />

<img width="1100" height="1236" alt="image" src="https://github.com/user-attachments/assets/62bf5352-0b98-40b0-b4ec-60d9110d7bdb" />

<img width="600" height="427" alt="image" src="https://github.com/user-attachments/assets/f6a555c8-67cb-4ffa-8ba4-20f2a9489b22" />

Prerequisites

    Docker and Docker Compose must be installed on your machine.

Usage Methodology
1. Start the Cluster

To build the necessary images and launch the Redis broker, controller, and agent services simultaneously, execute the following command from the project root:
Bash

docker-compose up --build

2. Access the Dashboard

The Capillary Controller provides a web-based dashboard for real-time monitoring. Once the containers are healthy, navigate to:

    URL: http://localhost:8080

    Features: View live CPU and memory metrics for all nodes, monitor liveness status, and manually trigger process restarts.

3. Scaling the Environment

To simulate a larger cluster, you can scale the number of monitoring agents dynamically:
Bash

docker-compose up --scale agent=3

Each agent container will independently report telemetry to the centralized controller.
4. Stop the System

To shut down the services and clean up the internal Docker network:
Bash

docker-compose down

Configuration

Key settings can be adjusted via environment variables in the docker-compose.yml file:

    BROKER_URL: Defines the Redis connection string (e.g., redis://redis:6379/0).

    NODE_ID: (Optional) Unique identifier for the agent; defaults to the container hostname if not set.


