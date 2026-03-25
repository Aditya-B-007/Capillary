import socket
import json
import time
from urllib.parse import urlparse
from common.config import settings

BROADCAST_IP = "255.255.255.255"
PORT = settings.discovery_port
parsed_broker = urlparse(settings.broker_url)
redis_host = parsed_broker.hostname or "redis"
redis_port = parsed_broker.port or 6379

MESSAGE = {
    "type": "CONTROLLER_ANNOUNCE",
    "redis_host": redis_host,
    "redis_port": redis_port
}

def broadcast():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    while True:
        sock.sendto(json.dumps(MESSAGE).encode(), (BROADCAST_IP, PORT))
        print(f"Broadcasted controller presence (Broker: {redis_host}:{redis_port})")
        time.sleep(2)

if __name__ == "__main__":
    broadcast()
