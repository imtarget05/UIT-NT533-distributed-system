#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  scenario_1_scalability.py — Kịch bản 1: Khả năng Co giãn (Scalability)   ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Thiết lập thử nghiệm:
  Đẩy tải tăng dần: 1 → 5 → 10 → 15 → 20 Chunk dữ liệu.
  Mỗi phase: gửi N chunks, đợi 30s, thu thập metrics từ Prometheus.

Metrics thu thập:
  - Số lượng Pod parallel-sort (kubectl + OpenFaaS API)
  - CPU Usage per Node (Prometheus node_cpu_seconds_total)
  - RPS (Requests Per Second) tới function (OpenFaaS gateway metrics)

Kết quả kỳ vọng:
  Khi tải tăng, OpenFaaS autoscaler (dựa vào RPS) nhân bản Pod.
  K3s scheduler rải Pod mới đều lên Worker1 (192.168.125.105) và Worker2 (192.168.125.106).
  CPU trên cả 2 Worker tăng đồng bộ → chứng minh load được chia sẻ hiệu quả.

Yêu cầu:
  pip install kafka-python requests tabulate

Cách chạy (từ máy ngoài cluster qua Tailscale):
  python scenario_1_scalability.py

  Hoặc chỉ định broker và Prometheus khác:
  python scenario_1_scalability.py --broker 192.168.125.104:30092 \\
                                    --prometheus http://192.168.125.104:30090
"""

import json
import time
import random
import argparse
import logging
import subprocess
import sys
from datetime import datetime

try:
    import requests
except ImportError:
    print("[ERROR] Thiếu thư viện 'requests'. Chạy: pip install requests")
    sys.exit(1)

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("[ERROR] Thiếu thư viện 'kafka-python'. Chạy: pip install kafka-python")
    sys.exit(1)

try:
    from tabulate import tabulate
except ImportError:
    # Fallback nếu chưa cài tabulate — dùng print thông thường
    tabulate = None

# ─── Cấu hình cluster ────────────────────────────────────────────────────────

# [NETWORKING] Địa chỉ Tailscale của các node (for external connections)
MASTER_IP  = "100.107.243.97"   # k3s-master  — Control Plane (Tailscale)
WORKER1_IP = "100.69.61.128"    # k3s-worker1 — Worker Node 1 (Tailscale)
WORKER2_IP = "100.108.56.79"    # k3s-worker2 — Worker Node 2 (Tailscale)

# [NETWORKING] LAN IPs — used for Prometheus instance label matching in PromQL
# node_exporter exports instance labels as LAN IPs (192.168.125.xxx:9100)
MASTER_LAN  = "192.168.125.104"
WORKER1_LAN = "192.168.125.105"
WORKER2_LAN = "192.168.125.106"

# Kafka Service — NodePort 30092 (chạy từ master OS qua Tailscale/LAN)
# Yêu cầu: Kafka phải được cấu hình advertised.listeners với EXTERNAL listener
# trỏ về NODE_IP:30092 để metadata trả đúng địa chỉ external.
DEFAULT_BROKER = f"{MASTER_IP}:30092"

# Prometheus NodePort — mặc định K3s kube-prometheus-stack dùng port 30090
DEFAULT_PROMETHEUS = f"http://{MASTER_IP}:30090"

# OpenFaaS Gateway NodePort
DEFAULT_GATEWAY = f"http://{MASTER_IP}:31112"

# Kafka topic
TOPIC = "UIT"
CHUNK_SIZE = 50_000

# Các phase tải: số lượng chunks gửi mỗi phase
LOAD_PHASES = [10, 50, 100, 150, 200]

# Thời gian chờ sau mỗi phase để HPA có thời gian scale (giây)
STABILIZATION_WAIT_S = 60

# Khoảng thời gian polling metrics sau khi gửi xong (giây)
# Đo CPU trong khi function vẫn đang xử lý chunks
METRICS_WAIT_S = 5

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# HÀM THU THẬP METRICS
# ══════════════════════════════════════════════════════════════════════════════

def query_prometheus(prometheus_url: str, promql: str) -> float | None:
    """
    Gọi Prometheus HTTP API với PromQL query.
    Trả về giá trị số đầu tiên hoặc None nếu lỗi.
    """
    try:
        resp = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": promql},
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", {}).get("result", [])
        if results:
            return float(results[0]["value"][1])
    except Exception as exc:
        logger.warning(f"Prometheus query lỗi (promql='{promql}'): {exc}")
    return None


def get_pod_count(namespace: str = "openfaas-fn") -> int:
    """
    Dùng kubectl để đếm số pod parallel-sort đang Running.
    Trả về 0 nếu kubectl không khả dụng.
    """
    try:
        result = subprocess.run(
            [
                "kubectl", "get", "pods",
                "-n", namespace,
                "-l", "faas_function=parallel-sort",
                "--field-selector", "status.phase=Running",
                "--no-headers",
            ],
            capture_output=True, text=True, timeout=10,
        )
        lines = [l for l in result.stdout.strip().split("\n") if l]
        return len(lines)
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning(f"kubectl không khả dụng: {exc}")
        return -1  # -1 = không đo được


def get_node_cpu_usage(prometheus_url: str, node_ip: str) -> float | None:
    """
    Lấy CPU usage % của một node cụ thể qua địa chỉ IP.

    PromQL: tỷ lệ CPU không idle trong 1 phút qua.
    1 - avg(rate(idle)) = avg(rate(non-idle)) = CPU utilization.
    """
    promql = (
        f'100 * (1 - avg by (instance) ('
        f'rate(node_cpu_seconds_total{{mode="idle",instance=~"{node_ip}:.*"}}[1m])'
        f'))'
    )
    return query_prometheus(prometheus_url, promql)


def get_openfaas_rps(gateway_url: str) -> float | None:
    """
    Lấy RPS (Requests Per Second) tới hàm parallel-sort qua OpenFaaS gateway.
    Endpoint: /system/functions (trả về metrics cho tất cả functions).
    """
    try:
        resp = requests.get(
            f"{gateway_url}/system/functions",
            auth=("admin", "TJjkh4ni8KXVAUL1cqnIU1FRaalmG1tS"),  # OpenFaaS basic-auth password
            timeout=5,
        )
        if resp.status_code == 200:
            for fn in resp.json():
                if fn.get("name") == "parallel-sort":
                    return float(fn.get("invocationCount", 0))
    except Exception as exc:
        logger.warning(f"Không lấy được OpenFaaS metrics: {exc}")
    return None


def collect_metrics(prometheus_url: str, gateway_url: str) -> dict:
    """Thu thập tất cả metrics của cluster tại thời điểm hiện tại."""
    return {
        "timestamp":       datetime.now().strftime("%H:%M:%S"),
        "pod_count":       get_pod_count(),
        "cpu_master_%":    get_node_cpu_usage(prometheus_url, MASTER_LAN),
        "cpu_worker1_%":   get_node_cpu_usage(prometheus_url, WORKER1_LAN),
        "cpu_worker2_%":   get_node_cpu_usage(prometheus_url, WORKER2_LAN),
        "fn_invocations":  get_openfaas_rps(gateway_url),
    }


def reset_to_min_pods(namespace: str = "openfaas-fn", target_replicas: int = 1,
                      wait_stable_s: int = 30, timeout_s: int = 120) -> bool:
    """
    Scale deployment parallel-sort xuống target_replicas trước khi test.
    Mục đích: đảm bảo HPA có thể chứng minh scale-OUT từ min→max pods.

    Nếu không reset, HPA có thể đã ở trạng thái 5 pods (do workload trước đó)
    và CPU/memory không bao giờ đủ cao để trigger scale thêm.

    Returns: True nếu thành công, False nếu lỗi (test vẫn tiếp tục).
    """
    logger.info(f"\n[PRE-TEST RESET] Scale parallel-sort → {target_replicas} pod để chứng minh scale-OUT...")
    try:
        # 1. Scale xuống target_replicas
        result = subprocess.run(
            ["kubectl", "scale", "deployment", "parallel-sort",
             "-n", namespace, f"--replicas={target_replicas}"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            logger.warning(f"  kubectl scale lỗi: {result.stderr.strip()}")
            return False
        logger.info(f"  ✓ kubectl scale → {target_replicas} replica(s) thành công")

        # 2. Chờ pod count ổn định
        logger.info(f"  Đợi pod count về {target_replicas} (timeout={timeout_s}s)...")
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            count = get_pod_count(namespace)
            logger.info(f"    Hiện tại: {count} pod(s) running...")
            if count == target_replicas:
                logger.info(f"  ✓ Pod count đạt {target_replicas}")
                break
            time.sleep(5)
        else:
            logger.warning(f"  ⚠ Timeout: pod count chưa về {target_replicas} sau {timeout_s}s — tiếp tục dù sao")

        # 3. Đợi thêm để Prometheus metrics và HPA ổn định
        logger.info(f"  Đợi thêm {wait_stable_s}s cho metrics ổn định...")
        time.sleep(wait_stable_s)
        logger.info("  [PRE-TEST RESET HOÀN THÀNH]\n")
        return True

    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning(f"  kubectl không khả dụng: {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# HÀM GỬI CHUNKS
# ══════════════════════════════════════════════════════════════════════════════

def create_producer(broker: str) -> KafkaProducer:
    """Khởi tạo KafkaProducer kết nối tới broker."""
    logger.info(f"Kết nối Kafka: {broker}")
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        compression_type="gzip",
        acks="all",
        retries=3,
        buffer_memory=67_108_864,   # 64MB buffer
        linger_ms=10,
    )


def send_n_chunks(producer: KafkaProducer, n_chunks: int, chunk_id_offset: int = 0) -> float:
    """
    Gửi n_chunks chunks vào Kafka topic.
    Trả về thời gian gửi (giây).
    """
    t0 = time.perf_counter()
    for i in range(n_chunks):
        chunk_id = chunk_id_offset + i
        data = random.choices(range(0, 10_000_000), k=CHUNK_SIZE)
        payload = {
            "chunk_id":     chunk_id,
            "total_chunks": n_chunks,
            "size":         CHUNK_SIZE,
            "timestamp_ms": int(time.time() * 1000),
            "data":         data,
        }
        future = producer.send(TOPIC, value=payload)
        future.get(timeout=30)  # Chờ broker confirm trước khi gửi chunk tiếp theo
        logger.info(f"  Đã gửi chunk {i+1}/{n_chunks} (chunk_id={chunk_id})")

    producer.flush()
    return time.perf_counter() - t0


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — Chạy kịch bản scalability
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kịch bản 1: Scalability Test — NT533",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--broker",     default=DEFAULT_BROKER)
    parser.add_argument("--prometheus", default=DEFAULT_PROMETHEUS)
    parser.add_argument("--gateway",    default=DEFAULT_GATEWAY)
    args = parser.parse_args()

    logger.info("═" * 70)
    logger.info("  Kịch bản 1: Scalability Test")
    logger.info(f"  Broker     : {args.broker}")
    logger.info(f"  Prometheus : {args.prometheus}")
    logger.info(f"  Gateway    : {args.gateway}")
    logger.info(f"  Phases     : {LOAD_PHASES} chunks/phase")
    logger.info("═" * 70)

    producer = create_producer(args.broker)
    all_results = []
    chunk_id_offset = 0

    # ── Pre-test reset: về 1 pod để HPA có thể chứng minh scale-OUT ────────────
    # Lý do: HPA có thể đang ở trạng thái N pods từ workload trước.
    # Nếu memory 66%/75% at idle → HPA giữ replicas hiện tại → không scale-out.
    # Reset về 1 pod → dưới tải, CPU pod đơn vượt 60% → HPA scale 1→N.
    reset_to_min_pods(target_replicas=1, wait_stable_s=30, timeout_s=120)

    # ── Baseline: thu metrics trước khi gửi bất kỳ tải nào ────────────────────
    logger.info("\n[BASELINE] Thu metrics trước khi gửi tải...")
    baseline = collect_metrics(args.prometheus, args.gateway)
    baseline["phase"] = "BASELINE"
    baseline["chunks_sent"] = 0
    baseline["send_time_s"] = 0.0
    all_results.append(baseline)
    logger.info(f"  Baseline → pod_count={baseline['pod_count']} | "
                f"CPU Worker1={baseline['cpu_worker1_%']:.1f}% | "
                f"CPU Worker2={baseline['cpu_worker2_%']:.1f}%"
                if baseline['cpu_worker1_%'] is not None else
                f"  Baseline → pod_count={baseline['pod_count']} (Prometheus không khả dụng)")

    # ── Từng phase tải ─────────────────────────────────────────────────────────
    for phase_idx, n_chunks in enumerate(LOAD_PHASES):
        logger.info(f"\n{'─' * 70}")
        logger.info(f"  PHASE {phase_idx + 1}/{len(LOAD_PHASES)}: Gửi {n_chunks} chunks...")
        logger.info(f"{'─' * 70}")

        # 1. Gửi chunks
        send_time = send_n_chunks(producer, n_chunks, chunk_id_offset)
        chunk_id_offset += n_chunks

        logger.info(f"  ✓ Gửi xong {n_chunks} chunks trong {send_time:.2f}s")
        logger.info(f"  Đợi {METRICS_WAIT_S}s để HPA và OpenFaaS autoscaler phản ứng...")
        time.sleep(METRICS_WAIT_S)

        # 2. Thu metrics ngay sau khi gửi
        metrics = collect_metrics(args.prometheus, args.gateway)
        metrics["phase"] = f"PHASE_{n_chunks}_chunks"
        metrics["chunks_sent"] = n_chunks
        metrics["send_time_s"] = round(send_time, 2)
        all_results.append(metrics)

        cpu_w1 = f"{metrics['cpu_worker1_%']:.1f}%" if metrics['cpu_worker1_%'] is not None else "N/A"
        cpu_w2 = f"{metrics['cpu_worker2_%']:.1f}%" if metrics['cpu_worker2_%'] is not None else "N/A"
        logger.info(
            f"  → Pods={metrics['pod_count']} | "
            f"CPU Worker1={cpu_w1} | CPU Worker2={cpu_w2} | "
            f"Invocations={metrics['fn_invocations']}"
        )

        # 3. Đợi ổn định trước phase tiếp theo
        if phase_idx < len(LOAD_PHASES) - 1:
            logger.info(f"  Đợi {STABILIZATION_WAIT_S}s ổn định trước phase tiếp theo...")
            time.sleep(STABILIZATION_WAIT_S)

    # ── In bảng kết quả ────────────────────────────────────────────────────────
    logger.info(f"\n{'═' * 70}")
    logger.info("  KẾT QUẢ TỔNG HỢP — Kịch bản 1: Scalability")
    logger.info("═" * 70)

    headers = ["Phase", "Chunks", "Pods", "CPU M%", "CPU W1%", "CPU W2%", "Invocations"]
    rows = []
    for r in all_results:
        rows.append([
            r["phase"],
            r["chunks_sent"],
            r["pod_count"],
            f"{r['cpu_master_%']:.1f}"   if r['cpu_master_%']  is not None else "N/A",
            f"{r['cpu_worker1_%']:.1f}"  if r['cpu_worker1_%'] is not None else "N/A",
            f"{r['cpu_worker2_%']:.1f}"  if r['cpu_worker2_%'] is not None else "N/A",
            r["fn_invocations"],
        ])

    if tabulate:
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print(" | ".join(headers))
        for row in rows:
            print(" | ".join(str(x) for x in row))

    # ── Phân tích kết quả ──────────────────────────────────────────────────────
    logger.info("\n  PHÂN TÍCH:")
    pod_counts = [r["pod_count"] for r in all_results if r["pod_count"] > 0]
    if len(pod_counts) >= 2 and pod_counts[-1] > pod_counts[0]:
        logger.info(f"  ✓ Scale OUT xác nhận: {pod_counts[0]} → {pod_counts[-1]} pods")
    else:
        logger.info(f"  ⚠ Scale OUT không quan sát được (pod_counts={pod_counts})")
        logger.info("    Kiểm tra: kubectl describe hpa parallel-sort-hpa -n openfaas-fn")

    cpu_w1_vals = [r["cpu_worker1_%"] for r in all_results if r["cpu_worker1_%"] is not None]
    cpu_w2_vals = [r["cpu_worker2_%"] for r in all_results if r["cpu_worker2_%"] is not None]
    if cpu_w1_vals and cpu_w2_vals:
        w1_max = max(cpu_w1_vals)
        w2_max = max(cpu_w2_vals)
        imbalance = abs(w1_max - w2_max)
        logger.info(f"  CPU Worker1 peak: {w1_max:.1f}%")
        logger.info(f"  CPU Worker2 peak: {w2_max:.1f}%")
        if imbalance < 20:
            logger.info(f"  ✓ Tải phân bổ đều: chênh lệch {imbalance:.1f}% (< 20%)")
        else:
            logger.info(f"  ⚠ Tải có thể không đều: chênh lệch {imbalance:.1f}%")

    producer.close()
    logger.info("\n  Kịch bản 1 hoàn thành.")


if __name__ == "__main__":
    main()
