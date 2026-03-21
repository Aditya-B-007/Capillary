import socket
import json
import time
import os

DISCOVERY_PORT = int(os.getenv("DISCOVERY_PORT", 9000))
DISCOVERY_TIMEOUT = 2  # seconds
CACHE_TTL = 60  # seconds


class ControllerDiscovery:
    def __init__(self, config=None):
        """
        config example:
        {
            "controller_ip": "192.168.1.10"
        }
        """
        self.config = config or {}
        self.cached_ip = None
        self.last_discovered = 0

    def discover(self):
        config_ip = self.config.get("controller_ip")
        if config_ip:
            return config_ip
        env_ip = os.getenv("CONTROLLER_IP")
        if env_ip:
            return env_ip
        if self._is_cache_valid():
            return self.cached_ip
        discovered_ip = self._broadcast_discover()

        if discovered_ip:
            self.cached_ip = discovered_ip
            self.last_discovered = time.time()

        return discovered_ip

    def invalidate_cache(self):
        self.cached_ip = None
        self.last_discovered = 0

    def _is_cache_valid(self):
        return (
            self.cached_ip is not None and
            (time.time() - self.last_discovered) < CACHE_TTL
        )

    def _broadcast_discover(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(DISCOVERY_TIMEOUT)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        try:
            message = json.dumps({
                "type": "DISCOVER_CONTROLLER"
            }).encode()

            sock.sendto(message, ("<broadcast>", DISCOVERY_PORT))

            while True:
                data, addr = sock.recvfrom(1024)
                try:
                    response = json.loads(data.decode())

                    if response.get("type") == "CONTROLLER_RESPONSE":
                        return addr[0]

                except json.JSONDecodeError:
                    continue

        except socket.timeout:
            return None

        finally:
            sock.close()