import logging
import os
import psutil
from common.models import Metrics

logger = logging.getLogger(__name__)

class MetricsCollector:
    def __init__(self, disk_path: str = "/"):
        self.disk_path = disk_path
        psutil.cpu_percent(interval=None) 

    def collect(self) -> Metrics:
        cpu_usage = psutil.cpu_percent(interval=None)
        cpu_times = psutil.cpu_times_percent(interval=None)
        cpu_stats = psutil.cpu_stats()
        load_avg = os.getloadavg()

        memory_usage = psutil.virtual_memory().percent
        swap_mem = psutil.swap_memory()

        disk_usage = self._safe_disk_usage()
        disk_io = psutil.disk_io_counters()

        net_io = psutil.net_io_counters()

        process_count = self._safe_process_count()
        uptime = self._safe_uptime()

        return Metrics(
            cpu_percent=cpu_usage,
            load_1m=load_avg[0],
            load_5m=load_avg[1],
            load_15m=load_avg[2],
            cpu_steal=getattr(cpu_times, 'steal', 0.0),
            cpu_user=cpu_times.user,
            cpu_system=cpu_times.system,
            context_switches=cpu_stats.ctx_switches,
            interrupts=cpu_stats.interrupts,
            memory_percent=memory_usage,
            swap_usage=swap_mem.percent,
            page_faults=0,
            memory_psi=0.0,
            disk_percent=disk_usage,
            disk_iops=disk_io.read_count + disk_io.write_count if disk_io else 0,
            disk_latency=0.0,
            disk_util=0.0,
            net_in=net_io.bytes_recv,
            net_out=net_io.bytes_sent,
            packet_drops=0,
            tcp_retransmits=0,
            fd_usage=0.0,
            thread_count=0,
            zombie_processes=0,
            process_count=process_count,
            uptime=uptime,
            disk_health=0.0,
            temperature=0.0,
            fan_speed=0.0,
            power_usage=0.0,
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

    def _safe_uptime(self) -> float:
        try:
            return psutil.boot_time()
        except Exception:
            logger.warning("Uptime metric unavailable")
            return 0.0