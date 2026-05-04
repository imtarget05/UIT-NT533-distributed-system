#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  scenario_1_scalability.py — Kịch bản 1: Khả năng Co giãn (Scalability)   ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Luồng MapReduce:
  [MAP]    Producer gửi N chunks chứa danh sách số ngẫu nhiên vào Kafka topic UIT.
           Kafka-connector đọc từng chunk → gọi OpenFaaS function parallel-sort.
           Function sort song song (Parallel Quicksort) → publish kết quả sorted
           vào Kafka topic UIT-OUTPUT.

  [REDUCE] Collector đọc N chunks đã sort từ UIT-OUTPUT.
           K-way merge (heapq.merge) gộp tất cả chunks thành 1 mảng sorted.
           Xác nhận kết quả: verify thứ tự tăng dần, in sample output.

Thiết lập thử nghiệm:
  Đẩy tải tăng dần: 10 → 50 → 100 → 150 → 200 Chunk dữ liệu.
  Mỗi phase: gửi N chunks, đợi xử lý, thu kết quả sorted, thu thập metrics.

Metrics thu thập:
  - Số lượng Pod parallel-sort (kubectl)
  - CPU Usage per Node (Prometheus)
  - Tổng thời gian sort (Map) + merge (Reduce)
  - Xác nhận sorted order của kết quả cuối

Kết quả kỳ vọng:
  Khi tải tăng, HPA scale parallel-sort pods 1 → N.
  K3s rải pods đều lên Worker1 và Worker2.
  UIT-OUTPUT nhận đủ N chunks đã sorted.
  K-way merge ra danh sách toàn bộ đã sắp xếp đúng.

Yêu cầu:
  pip install kafka-python requests tabulate

Cách chạy:
  python scenario_1_scalability.py
  python scenario_1_scalability.py --phases 5 10 20 --collect-timeout 120
"""

import json
import time
import random
import argparse
import logging
import subprocess
import sys
import heapq
from datetime import datetime

try:
    import requests
except ImportError:
    print("[ERROR] Thiếu thư viện 'requests'. Chạy: pip install requests")
    sys.exit(1)

try:
    from kafka import KafkaProducer, KafkaConsumer
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

# Các phase tải: số lượng chunks gửi mỗi phase (phân bổ hợp lý)
# Tổng: 10 + 50 + 100 + 150 + 200 = 510 chunks (khớp với Collector max)
LOAD_PHASES = [10, 50, 100, 150, 200]

# Thời gian chờ sau mỗi phase để HPA có thời gian scale (giây)
# Tăng từ 60s → 120s để HPA ổn định metrics và scale nodes if needed
STABILIZATION_WAIT_S = 120

# Khoảng thời gian polling metrics sau khi gửi xong (giây)
# Đo CPU trong khi function vẫn đang xử lý chunks
# Tăng từ 5s → 30s để chứa độ trễ connector processing + HPA reaction time
METRICS_WAIT_S = 30

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
import os as _os

# Đường dẫn log mặc định: <project_root>/demo_logs/scenario1.log
# Mở mode='w' → MỖI LẦN CHẠY GHI ĐÈ HOÀN TOÀN
_DEFAULT_LOG_DIR = _os.environ.get(
    "DEMO_LOG_DIR",
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "demo_logs"),
)
DEFAULT_LOG_FILE = _os.path.abspath(_os.path.join(_DEFAULT_LOG_DIR, "scenario1.log"))


def setup_demo_logging(log_file: str = DEFAULT_LOG_FILE) -> str:
    """Cấu hình root logger: in ra console + ghi đè vào log_file mỗi lần chạy."""
    _os.makedirs(_os.path.dirname(log_file), exist_ok=True)
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    root = logging.getLogger()
    # Xóa handler cũ (tránh duplicate khi import lại)
    for h in list(root.handlers):
        root.removeHandler(h)
    # Console
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)
    # File (mode='w' = ghi đè)
    fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    root.setLevel(logging.INFO)
    return log_file


# Gọi ngay khi import module để mọi log đều được ghi
setup_demo_logging()
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


def check_and_restore_hpa(namespace: str = "openfaas-fn", hpa_name: str = "parallel-sort-hpa") -> bool:
    """
    Kiểm tra HPA tồn tại. Nếu không → tự động tạo mới.
    CRITICAL: HPA must be created BEFORE we scale pods!
    Returns: True nếu HPA sẵn sàng, False nếu lỗi.
    """
    logger.info(f"\n[HPA CHECK] Kiểm tra HorizontalPodAutoscaler '{hpa_name}'...")
    hpa_created = False
    
    try:
        # Kiểm tra HPA có tồn tại không
        result = subprocess.run(
            ["kubectl", "get", "hpa", hpa_name, "-n", namespace],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            logger.info(f"  ✓ HPA '{hpa_name}' đã tồn tại")
        else:
            logger.warning(f"  ⚠ HPA không tìm thấy. Tự động khôi phục...")
            hpa_created = True
            
            # Tạo HPA manifest
            hpa_yaml = f"""apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {hpa_name}
  namespace: {namespace}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: parallel-sort
  minReplicas: 1
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 60
"""
            # Ghi vào temp file
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                f.write(hpa_yaml)
                temp_file = f.name
            
            try:
                result = subprocess.run(
                    ["kubectl", "apply", "-f", temp_file],
                    capture_output=True, text=True, timeout=10,
                )
                if result.returncode == 0:
                    logger.info(f"  ✓ HPA khôi phục thành công")
                else:
                    logger.warning(f"  ⚠ Lỗi tạo HPA: {result.stderr.strip()}")
                    return False
            finally:
                import os
                try:
                    os.unlink(temp_file)
                except:
                    pass
        
        # CRITICAL: Đợi đủ lâu để Kubernetes metrics server cập nhật metrics
        # Metrics cần ít nhất 15-20 giây để được collect lần đầu
        if hpa_created:
            logger.info(f"  ⏳ Đợi 45s để metrics server cập nhật (CRITICAL!)...")
            for i in range(45, 0, -5):
                logger.info(f"     {i}s còn lại...")
                time.sleep(5)
        else:
            logger.info(f"  ⏳ Đợi 10s để HPA ổn định...")
            time.sleep(10)
        
        return True
        
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning(f"  ⚠ kubectl không khả dụng: {exc}")
        return False


def check_preflight() -> bool:
    """
    Pre-flight diagnostics: kiểm tra Kafka, kubectl, Prometheus.
    Returns: True nếu mọi thứ ổn, False nếu có vấn đề (nhưng test vẫn chạy).
    """
    logger.info(f"\n[PRE-FLIGHT CHECK] Kiểm tra điều kiện trước khi test...")
    
    # 1. Kiểm tra kubectl
    try:
        result = subprocess.run(["kubectl", "cluster-info"], capture_output=True, timeout=5)
        if result.returncode == 0:
            logger.info(f"  ✓ kubectl & cluster: OK")
        else:
            logger.warning(f"  ⚠ kubectl cluster-info failed")
    except FileNotFoundError:
        logger.error(f"  ✗ kubectl không tìm thấy")
    
    # 2. Kiểm tra Kafka broker
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers='100.107.243.97:30092',
            request_timeout_ms=5000
        )
        producer.close()
        logger.info(f"  ✓ Kafka broker: OK")
    except Exception as e:
        logger.error(f"  ✗ Kafka broker: {e}")
    
    return True  # Vẫn tiếp tục dù sao


def reset_to_min_pods(namespace: str = "openfaas-fn", target_replicas: int = 1,
                      wait_stable_s: int = 60, timeout_s: int = 120) -> bool:
    """
    Scale deployment parallel-sort xuống target_replicas trước khi test.
    Mục đích: đảm bảo HPA có thể chứng minh scale-OUT từ min→max pods.

    CRITICAL: After scaling, we must wait for:
      1. Metrics collection (15-20s minimum)
      2. HPA to read those metrics (10-15s more)
      3. CPU to drop back to low level (before we apply load)
    
    Total wait: 60s minimum (increased from 30s due to HPA metric collection delay)

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

        # 3. CRITICAL: Đợi thêm để Prometheus metrics ổn định BEFORE HPA starts
        # This is essential - HPA needs stable metrics to make scaling decisions
        logger.info(f"  ⏳ Đợi {wait_stable_s}s cho metrics server ổn định (CRITICAL!)...")
        for i in range(wait_stable_s, 0, -10):
            logger.info(f"     {i}s còn lại - Metrics collecting...")
            time.sleep(10 if i > 10 else i)
        
        logger.info("  ✓ [PRE-TEST RESET HOÀN THÀNH] Metrics ready, pods stable, ready for load!")
        logger.info("  ✓ HPA is now ready to scale on demand\n")
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
    global LOAD_PHASES, STABILIZATION_WAIT_S, METRICS_WAIT_S
    parser = argparse.ArgumentParser(
        description="Kịch bản 1: Scalability Test — NT533",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--broker",     default=DEFAULT_BROKER)
    parser.add_argument("--prometheus", default=DEFAULT_PROMETHEUS)
    parser.add_argument("--gateway",    default=DEFAULT_GATEWAY)
    parser.add_argument("--phases", nargs="+", type=int, default=None,
                        help="Custom load phases, e.g. --phases 5 10 20")
    parser.add_argument("--collect-timeout", type=int, default=420,
                        dest="collect_timeout",
                        help="Timeout (s) for collecting sorted chunks from UIT-OUTPUT (420s = 7min for 510 chunks @ 1.2 chunks/sec)")
    parser.add_argument("--stabilize-wait", type=int, default=STABILIZATION_WAIT_S,
                        dest="stabilize_wait")
    parser.add_argument("--metrics-wait", type=int, default=METRICS_WAIT_S,
                        dest="metrics_wait")
    parser.add_argument("--output-topic", default="UIT-OUTPUT", dest="output_topic")
    parser.add_argument("--chunks-log",
                        default=_os.path.abspath(_os.path.join(_DEFAULT_LOG_DIR, "scenario1_chunks.log")),
                        dest="chunks_log",
                        help="Đường dẫn file ghi tóm tắt + chunk mẫu (ghi đè)")
    args = parser.parse_args()

    # Áp dụng phases tùy chọn
    if args.phases:
        LOAD_PHASES = args.phases
    STABILIZATION_WAIT_S = args.stabilize_wait
    METRICS_WAIT_S = args.metrics_wait

    logger.info("═" * 70)
    logger.info(f"  Kịch bản 1: Scalability Test — {datetime.now().isoformat()}")
    logger.info(f"  Log file   : {DEFAULT_LOG_FILE} (ghi đè mỗi lần chạy)")
    logger.info(f"  Broker     : {args.broker}")
    logger.info(f"  Prometheus : {args.prometheus}")
    logger.info(f"  Gateway    : {args.gateway}")
    logger.info(f"  Phases     : {LOAD_PHASES} chunks/phase")
    logger.info(f"  Collect timeout: {args.collect_timeout}s")
    logger.info("═" * 70)

    # PRE-FLIGHT: Kiểm tra điều kiện, khôi phục HPA nếu cần
    check_preflight()
    check_and_restore_hpa()
    
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

    # ── COLLECT PHASE: K-way Merge từ UIT-OUTPUT topic ──────────────────────────
    logger.info(f"\n{'─' * 70}")
    logger.info("  PHASE COLLECT/REDUCE — Gộp chunks đã sort thành mảng hoàn chỉnh")
    logger.info(f"{'─' * 70}")
    
    total_chunks_sent = sum(r["chunks_sent"] for r in all_results if "chunks_sent" in r and r["chunks_sent"] > 0)
    
    merged_data, chunks_dict, is_sorted = collect_sorted_chunks(
        broker=args.broker,
        output_topic=args.output_topic,
        n_chunks=total_chunks_sent,
        timeout_s=args.collect_timeout
    )

    # In TÓM TẮT chunks + chọn 1 chunk NGẪU NHIÊN để dump chi tiết
    sample_chunk_id = None
    sample_chunk = None
    if chunks_dict:
        logger.info("")
        logger.info(f"  {'═' * 70}")
        logger.info(f"  CHUNKS ĐÃ SẮP XẾP — Tổng: {len(chunks_dict)} chunk(s) thu được")
        logger.info(f"  {'═' * 70}")

        # Chọn ngẫu nhiên 1 chunk để in chi tiết (tránh tràn output)
        sample_chunk_id = random.choice(list(chunks_dict.keys()))
        sample_chunk = chunks_dict[sample_chunk_id]
        sample_data = sample_chunk.get('data', [])

        logger.info(f"\n  >>> CHUNK MẪU (ngẫu nhiên) — chunk_id={sample_chunk_id}")
        logger.info(f"      size            : {len(sample_data):,} phần tử")
        logger.info(f"      sort_ms         : {sample_chunk.get('sort_ms', 'N/A')}")
        logger.info(f"      total_chunks    : {sample_chunk.get('total_chunks', 'N/A')}")
        logger.info(f"      timestamp_ms    : {sample_chunk.get('timestamp_ms', 'N/A')}")
        if sample_data:
            logger.info(f"      min / max       : {min(sample_data)} / {max(sample_data)}")
            is_chunk_sorted = (sample_data == sorted(sample_data))
            logger.info(f"      is_sorted       : {'✅ YES' if is_chunk_sorted else '❌ NO'}")
            logger.info(f"      first 20 values : {sample_data[:20]}")
            logger.info(f"      last  20 values : {sample_data[-20:]}")

    # Ghi log: tóm tắt + 1 chunk mẫu ngẫu nhiên (đầy đủ data) — tránh tràn file
    if args.chunks_log and chunks_dict:
        try:
            with open(args.chunks_log, "w", encoding="utf-8") as f:
                f.write(f"# Scenario 1 — Sorted chunks summary log\n")
                f.write(f"# Generated: {datetime.now().isoformat()}\n")
                f.write(f"# Output topic: {args.output_topic}\n")
                f.write(f"# Total chunks collected: {len(chunks_dict)}\n")
                f.write(f"# Sample chunk (random): chunk_id={sample_chunk_id}\n\n")

                # Bảng tóm tắt tất cả chunks (1 dòng mỗi chunk)
                f.write("## SUMMARY (one line per chunk)\n")
                f.write(f"{'chunk_id':>9} | {'size':>7} | {'sort_ms':>9} | "
                        f"{'min':>10} | {'max':>10} | sorted?\n")
                f.write("-" * 80 + "\n")
                for cid in sorted(chunks_dict.keys(),
                                  key=lambda x: int(x) if str(x).isdigit() else x):
                    ch = chunks_dict[cid]
                    data = ch.get('data', [])
                    if data:
                        ok = "YES" if data == sorted(data) else "NO"
                        f.write(f"{str(cid):>9} | {len(data):>7,} | "
                                f"{str(ch.get('sort_ms','N/A')):>9} | "
                                f"{min(data):>10} | {max(data):>10} | {ok}\n")

                # CHUNK MẪU NGẪU NHIÊN — full data
                if sample_chunk is not None:
                    sample_data = sample_chunk.get('data', [])
                    f.write(f"\n## SAMPLE CHUNK (random pick) — chunk_id={sample_chunk_id}\n")
                    f.write(f"size={len(sample_data)}\n")
                    f.write(f"sort_ms={sample_chunk.get('sort_ms','N/A')}\n")
                    if sample_data:
                        f.write(f"min={min(sample_data)} max={max(sample_data)}\n")
                        f.write(f"is_sorted={sample_data == sorted(sample_data)}\n")
                        f.write(f"first50={sample_data[:50]}\n")
                        f.write(f"last50={sample_data[-50:]}\n")
                        f.write(f"middle50_at_idx_{len(sample_data)//2}="
                                f"{sample_data[len(sample_data)//2:len(sample_data)//2+50]}\n")

                if merged_data is not None:
                    f.write(f"\n## K-way merge result\n")
                    f.write(f"total_elements={len(merged_data)}\n")
                    f.write(f"is_sorted={is_sorted}\n")
                    if merged_data:
                        f.write(f"min={min(merged_data)} max={max(merged_data)}\n")
                        f.write(f"first20={merged_data[:20]}\n")
                        f.write(f"last20={merged_data[-20:]}\n")
            logger.info(f"\n  ✓ Đã ghi log chunks → {args.chunks_log}")
        except Exception as exc:
            logger.warning(f"  ⚠ Không ghi được chunks log: {exc}")

    if merged_data is not None:
        logger.info(f"\n  ✅ REDUCE THÀNH CÔNG")
        logger.info(f"     Total elements: {len(merged_data):,}")
        logger.info(f"     Is sorted: {'✅ YES' if is_sorted else '❌ NO'}")
        if not is_sorted:
            logger.warning("     ⚠️  Kết quả cuối KHÔNG sắp xếp đúng!")
    else:
        logger.warning(f"\n  ⚠️  Collect THẤT BẠI — không đọc được đủ chunks từ {args.output_topic}")
        logger.info("     Kiểm tra: Kafka topic UIT-OUTPUT tồn tại?")
        logger.info("     $ kubectl exec -n kafka my-kafka-broker-0 -- \\")
        logger.info("       kafka-topics.sh --list --bootstrap-server localhost:9092")

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


# ══════════════════════════════════════════════════════════════════════════════
# COLLECT PHASE — K-way Merge (MapReduce Reduce Step)
# ══════════════════════════════════════════════════════════════════════════════

def collect_sorted_chunks(broker: str, output_topic: str, n_chunks: int, 
                         timeout_s: int = 420) -> tuple:
    """
    Đọc N chunks đã sort từ Kafka topic OUTPUT (UIT-OUTPUT).
    Thực hiện k-way merge để gộp thành 1 mảng đã sort hoàn chỉnh.
    
    Returns: (sorted_array, chunks_dict, is_valid)
      - sorted_array: danh sách đã sort [0, 1, 2, ...]
      - chunks_dict: {'chunk_id': {'data': [...], 'sort_ms': 123.4, ...}}
      - is_valid: True nếu kết quả đúng, False nếu có lỗi
    """
    logger.info(f"\n[COLLECT PHASE] Đợi {n_chunks} sorted chunks từ {output_topic}...")
    
    chunks_dict = {}
    received = 0
    t_start = time.time()
    
    try:
        consumer = KafkaConsumer(
            output_topic,
            bootstrap_servers=broker,
            group_id=f'scenario1-collector-{int(time.time())}',  # Unique group để không conflict
            auto_offset_reset='earliest',  # Đọc từ đầu topic
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=30000,  # Timeout: 30s (tăng từ 5s để chứa độ trễ xử lý connector)
            session_timeout_ms=60000,   # Session: 60s (tránh rebalancing)
            max_poll_records=500,       # Lấy tối đa 500 records mỗi poll
        )
        
        for msg in consumer:
            try:
                chunk = msg.value
                chunk_id = chunk.get('chunk_id')
                data = chunk.get('data', [])
                
                chunks_dict[chunk_id] = chunk
                received += 1
                
                logger.info(
                    f"  [{received:>2}/{n_chunks}] Received chunk_id={chunk_id} | "
                    f"size={len(data)} | sort_ms={chunk.get('sort_ms', 'N/A')}"
                )
                
                if received >= n_chunks:
                    break
                    
            except Exception as e:
                logger.warning(f"  Parse error: {e}")
        
        consumer.close()
        
    except Exception as e:
        logger.error(f"  Consumer error: {e}")
        return None, chunks_dict, False
    
    elapsed = time.time() - t_start
    logger.info(f"  ✓ Collected {received}/{n_chunks} chunks trong {elapsed:.2f}s")
    
    # K-way Merge: gộp tất cả chunks thành 1 mảng sorted
    if received < n_chunks:
        logger.warning(f"  ⚠ Chỉ nhận {received}/{n_chunks} chunks — có thể thiếu")
        return None, chunks_dict, False
    
    # Trích xuất data từ mỗi chunk (giữ theo thứ tự chunk_id)
    sorted_chunks = sorted(chunks_dict.items(), key=lambda x: x[0])
    chunk_arrays = [chunk[1].get('data', []) for chunk in sorted_chunks]
    
    # K-way merge bằng heapq.merge (efficient O(N log K) where K=20 chunks)
    merged = list(heapq.merge(*chunk_arrays))
    
    # Xác minh: kết quả cuối cùng phải đã sort
    is_sorted = (merged == sorted(merged))
    
    logger.info(f"\n  [VERIFY] K-way merge result:")
    logger.info(f"    Total elements: {len(merged):,}")
    logger.info(f"    Is sorted: {'✅ YES' if is_sorted else '❌ NO'}")
    
    if len(merged) > 0:
        logger.info(f"    Min value: {min(merged)}")
        logger.info(f"    Max value: {max(merged)}")
        logger.info(f"    First 10: {merged[:10]}")
        logger.info(f"    Last 10:  {merged[-10:]}")
    
    return merged, chunks_dict, is_sorted


# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
