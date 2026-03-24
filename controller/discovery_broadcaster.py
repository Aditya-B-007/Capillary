import socket
import json
import time

BROADCAST_IP = "255.255.255.255"
PORT = 9999 #Import from config file

MESSAGE = {
    "type": "CONTROLLER_ANNOUNCE",
    "redis_host": "192.168.1.10",   # change dynamically if needed
    "redis_port": 6379 #Import from the config file
}

def broadcast():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    while True:
        sock.sendto(json.dumps(MESSAGE).encode(), (BROADCAST_IP, PORT))
        print("Broadcasted controller presence")
        time.sleep(2)

if __name__ == "__main__":
    broadcast()