import socket
import json
import threading
import os
DISCOVERY_PORT = os.getenv("DISCOVERY_PORT", 9000)

class DiscoveryListener:
    def __init__(self, controller_id="controller-1"):
        self.controller_id = controller_id
        self._running = False

    def start(self):
        """Start discovery listener in a background thread"""
        if self._running:
            return

        self._running = True
        thread = threading.Thread(target=self._listen, daemon=True)
        thread.start()

    def _listen(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("", DISCOVERY_PORT))

        while self._running:
            try:
                data, addr = sock.recvfrom(1024)

                try:
                    message = json.loads(data.decode())
                except json.JSONDecodeError:
                    continue

                if message.get("type") == "DISCOVER_CONTROLLER":
                    response = {
                        "type": "CONTROLLER_RESPONSE",
                        "controller_id": self.controller_id
                    }

                    sock.sendto(json.dumps(response).encode(), addr)

            except Exception:
                continue

    def stop(self):
        self._running = False