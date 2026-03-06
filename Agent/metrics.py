import logging
import psutil
from common.models import Metrics

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self, disk_path: str = "/"):
        self.disk_path = disk_path
        psutil.cpu_percent(interval=None) 

    def collect(self) -> Metrics:
        cpu_usage = psutil.cpu_percent(interval=None)
        memory_usage = psutil.virtual_memory().percent

        disk_usage = self._safe_disk_usage()
        active_procs = self._safe_process_count()

        return Metrics(
            cpu_percent=cpu_usage,
            memory_percent=memory_usage,
            disk_percent=disk_usage,
            active_processes=active_procs
        )

    def _safe_disk_usage(self) -> float:
        try:
            return psutil.disk_usage(self.disk_path).percent
        except Exception:
            logger.warning("Disk metric unavailable")
            return 0.0

    def _safe_process_count(self) -> int:
        try:
            return len(psutil.pids())
        except Exception:
            logger.warning("Process metric unavailable")
            return 0