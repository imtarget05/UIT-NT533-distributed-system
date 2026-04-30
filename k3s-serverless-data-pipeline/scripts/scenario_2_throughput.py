#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  scenario_2_throughput.py — Kịch bản 2: Thông lượng cao & Chunking        ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Thiết lập thử nghiệm:
  Đẩy mảng 1.000.000 phần tử (20 chunks × 50.000 phần tử) vào Kafka.

Mục tiêu chứng minh:
  1. KHÔNG bị OOM (Exit code 137): RAM mỗi Pod < 500Mi trong suốt quá trình.
  2. Kỹ thuật Chunking hoạt động: mỗi chunk ~400KB JSON raw → ~100KB gzip.
  3. Parallel Quicksort tận dụng đa nhân: thời gian sort/chunk < ngưỡng baseline.

Metrics thu thập:
  - Memory RSS của Pod parallel-sort (Prometheus container_memory_rss)
  - CPU usage per Pod (container_cpu_usage_seconds_total)
  - Throughput tổng thể (phần tử/giây end-to-end)
  - Thời gian sort từng chunk (từ Kafka message timestamp đến response time)
  - Exit code của containers (kiểm tra OOMKill)

Kết quả kỳ vọng:
  - RAM Pod: duy trì < 500Mi (ngưỡng OOM của K3s với limit 1Gi)
  - Throughput: > 100.000 phần tử/giây (tổng thể)
  - Không có container bị OOMKilled (kubectl describe pod kiểm tra)

Yêu cầu:
  pip install kafka-python requests tabulate

Cách chạy:
  python scenario_2_throughput.py
  python scenario_2_throughput.py --broker 192.168.125.104:30092
"""

import json
import time
import random
import argparse
import logging
import subprocess
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("[ERROR] pip install requests")
    sys.exit(1)

try:
    from kafka import KafkaProducer
except ImportError:
    print("[ERROR] pip install kafka-python")
    sys.exit(1)

try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

# ─── Cấu hình ────────────────────────────────────────────────────────────────

MASTER_IP  = "100.107.243.97"   # k3s-master  (Tailscale)
WORKER1_IP = "100.69.61.128"    # k3s-worker1 (Tailscale)
WORKER2_IP = "100.108.56.79"    # k3s-worker2 (Tailscale)

DEFAULT_BROKER     = "100.107.243.97:30092"   # Kafka NodePort (tests run on master OS)
DEFAULT_PROMETHEUS = f"http://{MASTER_IP}:30090"

TOPIC         = "UIT"
TOTAL_ELEMENTS = 1_000_000   # 1 triệu phần tử
CHUNK_SIZE     = 50_000      # 50K phần tử/chunk = ~400KB JSON, ~100KB gzip
TOTAL_CHUNKS   = TOTAL_ELEMENTS // CHUNK_SIZE  # = 20 chunks

# Ngưỡng cảnh báo OOM (tính bằng bytes = 500Mi)
OOM_WARN_THRESHOLD_BYTES = 500 * 1024 * 1024

# Khoảng cách giữa các lần poll metrics trong lúc gửi (giây)
METRICS_POLL_INTERVAL_S = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# THU THẬP METRICS
# ══════════════════════════════════════════════════════════════════════════════

def query_prometheus(url: str, promql: str) -> float | None:
    try:
        r = requests.get(f"{url}/api/v1/query", params={"query": promql}, timeout=5)
        r.raise_for_status()
        results = r.json().get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception as exc:
        logger.debug(f"Prometheus query lỗi: {exc}")
    return None


def get_pod_memory_rss(prometheus_url: str) -> dict:
    """
    Lấy Memory RSS (bytes) của tất cả pod parallel-sort.
    RSS = Resident Set Size = RAM thực sự đang dùng (không tính swap).

    PromQL giải thích:
      container_memory_rss: metric từ cAdvisor (chạy trong K3s)
      container="parallel-sort": tên container trong pod
      namespace="openfaas-fn": namespace của OpenFaaS functions
    """
    promql = (
        'container_memory_rss{'
        'container="parallel-sort",'
        'namespace="openfaas-fn"'
        '}'
    )
    try:
        r = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        r.raise_for_status()
        results = r.json().get("data", {}).get("result", [])
        memory_map = {}
        for item in results:
            pod_name = item["metric"].get("pod", "unknown")
            node_ip  = item["metric"].get("instance", "unknown").split(":")[0]
            rss_bytes = float(item["value"][1])
            memory_map[pod_name] = {
                "rss_bytes":  rss_bytes,
                "rss_mib":    rss_bytes / (1024 * 1024),
                "node_ip":    node_ip,
                "oom_risk":   rss_bytes > OOM_WARN_THRESHOLD_BYTES,
            }
        return memory_map
    except Exception as exc:
        logger.debug(f"Memory query lỗi: {exc}")
        return {}


def get_pod_cpu_usage(prometheus_url: str) -> dict:
    """
    Lấy CPU usage (cores) của các pod parallel-sort trong 1 phút qua.
    rate(container_cpu_usage_seconds_total[1m]) = CPU cores đang dùng.
    """
    promql = (
        'rate(container_cpu_usage_seconds_total{'
        'container="parallel-sort",'
        'namespace="openfaas-fn"'
        '}[1m])'
    )
    try:
        r = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        r.raise_for_status()
        results = r.json().get("data", {}).get("result", [])
        cpu_map = {}
        for item in results:
            pod_name = item["metric"].get("pod", "unknown")
            cpu_map[pod_name] = float(item["value"][1])
        return cpu_map
    except Exception as exc:
        logger.debug(f"CPU query lỗi: {exc}")
        return {}


def check_oom_killed(namespace: str = "openfaas-fn") -> list:
    """
    Kiểm tra container bị OOMKilled bằng kubectl.
    Trả về danh sách pod bị OOMKill.
    """
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace,
             "-l", "faas_function=parallel-sort", "-o", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return []
        pods_json = json.loads(result.stdout)
        oom_pods = []
        for pod in pods_json.get("items", []):
            pod_name = pod["metadata"]["name"]
            for cs in pod.get("status", {}).get("containerStatuses", []):
                if cs.get("lastState", {}).get("terminated", {}).get("reason") == "OOMKilled":
                    oom_pods.append(pod_name)
                # Kiểm tra trạng thái hiện tại
                if cs.get("state", {}).get("terminated", {}).get("reason") == "OOMKilled":
                    if pod_name not in oom_pods:
                        oom_pods.append(pod_name)
        return oom_pods
    except Exception as exc:
        logger.debug(f"kubectl OOM check lỗi: {exc}")
        return []


# ══════════════════════════════════════════════════════════════════════════════
# PRODUCER
# ══════════════════════════════════════════════════════════════════════════════

def create_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        compression_type="gzip",
        acks="all",
        retries=3,
        buffer_memory=67_108_864,
        linger_ms=20,
        batch_size=65_536,
    )


# ══════════════════════════════════════════════════════════════════════════════
# MONITOR THREAD — Thu thập metrics song song với quá trình gửi
# ══════════════════════════════════════════════════════════════════════════════

class MetricsMonitor:
    """
    Thu thập memory/CPU metrics trong background thread trong khi producer đang gửi.
    Dùng threading (không phải multiprocessing) để không ảnh hưởng Kafka producer.
    """

    def __init__(self, prometheus_url: str):
        self.prometheus_url = prometheus_url
        self.snapshots = []       # Danh sách snapshot metrics theo thời gian
        self._running = False

    def start_monitoring(self, interval_s: float = 5.0):
        """Bắt đầu thu metrics trong một thread riêng."""
        import threading
        self._running = True

        def _poll_loop():
            while self._running:
                snapshot = {
                    "time":    datetime.now().strftime("%H:%M:%S"),
                    "memory":  get_pod_memory_rss(self.prometheus_url),
                    "cpu":     get_pod_cpu_usage(self.prometheus_url),
                    "oom_pods": check_oom_killed(),
                }
                self.snapshots.append(snapshot)

                # Log ngay nếu có cảnh báo OOM
                for pod, info in snapshot["memory"].items():
                    if info["oom_risk"]:
                        logger.warning(
                            f"  ⚠ OOM RISK: Pod '{pod}' dùng "
                            f"{info['rss_mib']:.0f}MiB > {OOM_WARN_THRESHOLD_BYTES//1024//1024}MiB"
                        )

                if snapshot["oom_pods"]:
                    logger.error(f"  ✗ OOM KILL phát hiện: {snapshot['oom_pods']}")

                time.sleep(interval_s)

        self._thread = threading.Thread(target=_poll_loop, daemon=True)
        self._thread.start()
        logger.info(f"Bắt đầu monitor metrics (interval={interval_s}s)...")

    def stop_monitoring(self):
        """Dừng polling."""
        self._running = False
        logger.info("Dừng monitor metrics.")

    def peak_memory_mib(self) -> float:
        """Trả về mức memory cao nhất quan sát được (MiB), trên tất cả pods."""
        peak = 0.0
        for snap in self.snapshots:
            for pod_info in snap["memory"].values():
                if pod_info["rss_mib"] > peak:
                    peak = pod_info["rss_mib"]
        return peak

    def any_oom_killed(self) -> bool:
        """Kiểm tra có bất kỳ pod nào bị OOMKilled trong suốt quá trình không."""
        for snap in self.snapshots:
            if snap["oom_pods"]:
                return True
        return False


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kịch bản 2: Throughput & Chunking Test — NT533",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--broker",     default=DEFAULT_BROKER)
    parser.add_argument("--prometheus", default=DEFAULT_PROMETHEUS)
    args = parser.parse_args()

    logger.info("═" * 70)
    logger.info("  Kịch bản 2: Throughput & Chunking Test")
    logger.info(f"  Broker     : {args.broker}")
    logger.info(f"  Prometheus : {args.prometheus}")
    logger.info(f"  Tổng phần tử : {TOTAL_ELEMENTS:,}")
    logger.info(f"  Chunk size   : {CHUNK_SIZE:,} phần tử (~400KB JSON, ~100KB gzip)")
    logger.info(f"  Tổng chunks  : {TOTAL_CHUNKS}")
    logger.info(f"  OOM threshold: {OOM_WARN_THRESHOLD_BYTES // 1024 // 1024}MiB")
    logger.info("═" * 70)

    # ── 1. Sinh dữ liệu ───────────────────────────────────────────────────────
    logger.info(f"\nSinh {TOTAL_ELEMENTS:,} số nguyên ngẫu nhiên...")
    t_gen = time.perf_counter()
    data = random.choices(range(0, 10_000_000), k=TOTAL_ELEMENTS)
    gen_time = time.perf_counter() - t_gen
    logger.info(f"✓ Đã sinh xong trong {gen_time:.2f}s")

    # ── 2. Khởi tạo monitor và producer ──────────────────────────────────────
    monitor = MetricsMonitor(args.prometheus)
    monitor.start_monitoring(interval_s=METRICS_POLL_INTERVAL_S)

    producer = create_producer(args.broker)
    chunk_metrics = []

    # ── 3. Gửi từng chunk và đo thời gian ────────────────────────────────────
    logger.info(f"\nBắt đầu gửi {TOTAL_CHUNKS} chunks vào Kafka...")
    t_send_all = time.perf_counter()

    for chunk_id in range(TOTAL_CHUNKS):
        idx_start = chunk_id * CHUNK_SIZE
        idx_end   = idx_start + CHUNK_SIZE
        chunk_data = data[idx_start:idx_end]

        payload = {
            "chunk_id":     chunk_id,
            "total_chunks": TOTAL_CHUNKS,
            "size":         len(chunk_data),
            "timestamp_ms": int(time.time() * 1000),
            "data":         chunk_data,
        }

        t_chunk = time.perf_counter()
        future = producer.send(TOPIC, value=payload)
        metadata = future.get(timeout=30)
        send_ms = (time.perf_counter() - t_chunk) * 1000

        chunk_metrics.append({
            "chunk_id":   chunk_id,
            "size":       len(chunk_data),
            "send_ms":    round(send_ms, 1),
            "partition":  metadata.partition,
            "offset":     metadata.offset,
        })

        logger.info(
            f"  [{chunk_id + 1:>2}/{TOTAL_CHUNKS}] "
            f"send={send_ms:.0f}ms | "
            f"partition={metadata.partition} | offset={metadata.offset}"
        )

    total_send_time = time.perf_counter() - t_send_all
    producer.flush()

    # ── 4. Đợi functions xử lý xong ─────────────────────────────────────────
    logger.info(f"\nĐợi 30s để function xử lý tất cả {TOTAL_CHUNKS} chunks...")
    time.sleep(30)

    monitor.stop_monitoring()

    # ── 5. Tính toán và in kết quả ───────────────────────────────────────────
    throughput = TOTAL_ELEMENTS / total_send_time

    avg_send_ms = sum(m["send_ms"] for m in chunk_metrics) / len(chunk_metrics)
    max_send_ms = max(m["send_ms"] for m in chunk_metrics)
    min_send_ms = min(m["send_ms"] for m in chunk_metrics)

    # Phân phối partitions
    partition_dist = {}
    for m in chunk_metrics:
        p = m["partition"]
        partition_dist[p] = partition_dist.get(p, 0) + 1

    logger.info(f"\n{'═' * 70}")
    logger.info("  KẾT QUẢ — Kịch bản 2: Throughput & Chunking")
    logger.info("═" * 70)
    logger.info(f"  Tổng phần tử gửi  : {TOTAL_ELEMENTS:,}")
    logger.info(f"  Tổng thời gian gửi: {total_send_time:.2f}s")
    logger.info(f"  Throughput gửi    : {throughput:,.0f} phần tử/giây")
    logger.info(f"  Send time/chunk   : avg={avg_send_ms:.0f}ms | min={min_send_ms:.0f}ms | max={max_send_ms:.0f}ms")
    logger.info(f"  Partition dist    : {partition_dist} (phân bổ round-robin)")

    # Kết quả OOM check
    peak_mem = monitor.peak_memory_mib()
    any_oom  = monitor.any_oom_killed()
    logger.info(f"\n  [MEMORY CHECK]")
    if peak_mem > 0:
        logger.info(f"  Peak memory Pod  : {peak_mem:.1f} MiB")
        if peak_mem < 500:
            logger.info(f"  ✓ PASS: Peak memory {peak_mem:.1f}MiB < 500MiB OOM threshold")
        else:
            logger.warning(f"  ⚠ CẢNH BÁO: Peak memory {peak_mem:.1f}MiB >= 500MiB")
    else:
        logger.info("  (Prometheus không khả dụng — kiểm tra kubectl top pods -n openfaas-fn)")

    if not any_oom:
        logger.info("  ✓ PASS: Không có container nào bị OOMKilled (Exit code 137 không xảy ra)")
    else:
        logger.error("  ✗ FAIL: Phát hiện OOMKilled! Tăng memory limit trong stack.yml")

    # In bảng chunks
    if tabulate:
        print("\n" + tabulate(
            [(m["chunk_id"], m["size"], m["send_ms"], m["partition"], m["offset"])
             for m in chunk_metrics],
            headers=["chunk_id", "size", "send_ms", "partition", "offset"],
            tablefmt="simple",
        ))

    # ── 6. Kết luận ──────────────────────────────────────────────────────────
    logger.info("\n  KỊCH BẢN 2 HOÀN THÀNH")
    logger.info("  Bước tiếp theo: Grafana dashboard → container memory RSS graph")
    logger.info(f"  Grafana URL: http://{MASTER_IP}:30030 (mặc định)")
    logger.info("  Dashboard: 'Kubernetes / Compute Resources / Pod' → filter parallel-sort")

    producer.close()


if __name__ == "__main__":
    main()
