import socket
import json
from common.config import settings

PORT = settings.discovery_port

def listen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", PORT))

    print("Listening for controller broadcast...")

    while True:
        data, addr = sock.recvfrom(1024)
        message = json.loads(data.decode())

        if message.get("type") == "CONTROLLER_ANNOUNCE":
            print(f"Controller found at {addr}")
            return message  # return redis config

if __name__ == "__main__":
    listen()