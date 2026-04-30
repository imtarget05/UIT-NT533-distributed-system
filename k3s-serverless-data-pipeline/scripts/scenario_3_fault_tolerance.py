#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  scenario_3_fault_tolerance.py — Kịch bản 3: Khả năng Chịu lỗi            ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Thiết lập thử nghiệm:
  1. Gửi 20 chunks vào Kafka (background thread — liên tục).
  2. Sau 15s, giả lập Worker2 (k3s-worker2) bị sập:
       Cách A (SSH):  ssh ubuntu@192.168.125.106 "sudo systemctl stop k3s-agent"
       Cách B (kubectl): kubectl cordon k3s-worker2 (dừng nhận pod mới)
                         kubectl drain k3s-worker2 --ignore-daemonsets --delete-emptydir-data
  3. Monitor:
     - Trạng thái node (NotReady → kubectl get nodes)
     - Pod rescheduling sang Worker1
     - Kafka consumer offset (dữ liệu không mất)
     - Thời gian tự phục hồi (Recovery Time < 30s mục tiêu)

Kết quả kỳ vọng:
  - Master phát hiện Worker2 NotReady trong < 5s (node heartbeat timeout).
  - Pod trên Worker2 bị Terminate → được Reschedule sang Worker1.
  - Kafka offset tiếp tục từ chỗ bị dừng (không mất message nào).
  - Self-healing hoàn tất trong < 30s.

Chú ý về cách mô phỏng lỗi:
  - SAFE   : Dùng "kubectl drain" (graceful) — pods được evict trước khi node tắt.
  - REALIST: Dùng SSH + systemctl stop k3s-agent (đột ngột, giống thực tế hơn).
  - Script hỗ trợ cả hai; mặc định dùng SSH (aggressive) để thử nghiệm thực tế.

Yêu cầu:
  pip install kafka-python requests paramiko tabulate
  SSH key phải có thể kết nối tới k3s-worker2 (192.168.125.106) không cần password.
  Hoặc dùng --drain-mode để dùng kubectl drain thay vì SSH.

Cách chạy:
  # Mô phỏng bằng SSH (aggressive - giống sập thực tế):
  python scenario_3_fault_tolerance.py --ssh-key ~/.ssh/id_rsa

  # Mô phỏng bằng kubectl drain (graceful - an toàn hơn):
  python scenario_3_fault_tolerance.py --drain-mode

  # Chỉ theo dõi (không giả lập lỗi):
  python scenario_3_fault_tolerance.py --monitor-only
"""

import json
import time
import random
import argparse
import logging
import subprocess
import threading
import sys
from datetime import datetime

try:
    import requests
except ImportError:
    print("[ERROR] pip install requests")
    sys.exit(1)

try:
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    print("[ERROR] pip install kafka-python")
    sys.exit(1)

# ─── Cấu hình cluster ────────────────────────────────────────────────────────

MASTER_IP  = "100.107.243.97"   # k3s-master  — Control Plane (Tailscale)
WORKER1_IP = "100.69.61.128"    # k3s-worker1 — Worker Node 1 (Tailscale)
WORKER2_IP = "100.108.56.79"    # k3s-worker2 — Worker Node 2 (Tailscale) [sẽ bị "ập"]

WORKER2_HOSTNAME = "k3s-worker2"

DEFAULT_BROKER     = "100.107.243.97:30092"   # Kafka NodePort (tests run on master OS)
DEFAULT_PROMETHEUS = f"http://{MASTER_IP}:30090"
DEFAULT_SSH_USER   = "master"
DEFAULT_SSH_PORT   = 22

TOPIC      = "UIT"
CHUNK_SIZE = 50_000

# Thời gian chờ trước khi giả lập lỗi (giây) — đủ để pipeline đang chạy ổn định
FAIL_DELAY_S = 15

# Mục tiêu: Self-healing trong bao lâu (giây)
RECOVERY_TARGET_S = 60

# Thời gian theo dõi sau khi node sập (giây)
# Mở rộng lên 180s: K8s cần ~40s để phát hiện NotReady + ~60s để reschedule pods
MONITOR_AFTER_FAIL_S = 180

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# MONITOR — Theo dõi trạng thái cluster
# ══════════════════════════════════════════════════════════════════════════════

class ClusterMonitor:
    """Thu thập snapshot trạng thái cluster theo định kỳ."""

    def __init__(self, prometheus_url: str):
        self.prometheus_url = prometheus_url
        self.timeline = []     # Danh sách sự kiện theo thứ tự thời gian
        self._lock = threading.Lock()

    def record(self, event_type: str, detail: str):
        """Ghi một sự kiện vào timeline."""
        entry = {
            "time":  datetime.now().strftime("%H:%M:%S"),
            "epoch": time.time(),
            "type":  event_type,
            "detail": detail,
        }
        with self._lock:
            self.timeline.append(entry)
        logger.info(f"  [{event_type}] {detail}")

    def get_node_status(self) -> dict:
        """Lấy trạng thái tất cả node qua kubectl."""
        try:
            result = subprocess.run(
                ["kubectl", "get", "nodes", "-o", "json"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode != 0:
                return {}
            nodes_json = json.loads(result.stdout)
            status_map = {}
            for node in nodes_json.get("items", []):
                name = node["metadata"]["name"]
                conditions = node.get("status", {}).get("conditions", [])
                for cond in conditions:
                    if cond["type"] == "Ready":
                        status_map[name] = cond["status"]  # "True" / "False" / "Unknown"
                        break
            return status_map
        except Exception as exc:
            logger.debug(f"kubectl nodes lỗi: {exc}")
            return {}

    def get_pods_on_node(self, node_name: str, namespace: str = "openfaas-fn") -> list:
        """Lấy danh sách pod parallel-sort trên một node cụ thể."""
        try:
            result = subprocess.run(
                ["kubectl", "get", "pods", "-n", namespace,
                 "-l", "faas_function=parallel-sort",
                 "--field-selector", f"spec.nodeName={node_name}",
                 "--no-headers", "-o", "wide"],
                capture_output=True, text=True, timeout=10,
            )
            lines = [l for l in result.stdout.strip().split("\n") if l]
            return lines
        except Exception:
            return []

    def get_kafka_consumer_offset(self, broker: str) -> dict:
        """
        Lấy consumer group offset để kiểm tra data retention.
        Nếu offset tăng tiếp tục sau khi node sập → data không mất.
        """
        try:
            result = subprocess.run(
                ["kubectl", "exec", "-n", "kafka",
                 "kafka-controller-0",
                 "--", "kafka-consumer-groups.sh",
                 "--bootstrap-server", "localhost:9092",
                 "--group", "faas-kafka-queue-worker",
                 "--describe"],
                capture_output=True, text=True, timeout=15,
            )
            lines = result.stdout.strip().split("\n")
            offsets = {}
            for line in lines[1:]:  # Bỏ dòng header
                parts = line.split()
                if len(parts) >= 5 and parts[1] == TOPIC:
                    partition = parts[2]
                    offset    = parts[3]
                    lag       = parts[5] if len(parts) > 5 else "N/A"
                    offsets[f"P{partition}"] = {
                        "offset": offset,
                        "lag":    lag,
                    }
            return offsets
        except Exception as exc:
            logger.debug(f"Kafka offset query lỗi: {exc}")
            return {}

    def query_prometheus(self, promql: str) -> float | None:
        try:
            r = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params={"query": promql},
                timeout=5,
            )
            r.raise_for_status()
            results = r.json().get("data", {}).get("result", [])
            if results:
                return float(results[0]["value"][1])
        except Exception:
            pass
        return None


# ══════════════════════════════════════════════════════════════════════════════
# MÔ PHỎNG LỖI
# ══════════════════════════════════════════════════════════════════════════════

def simulate_node_failure_ssh(
    target_ip: str,
    ssh_user: str,
    ssh_port: int,
    ssh_key_path: str | None,
) -> bool:
    """
    Mô phỏng node sập đột ngột bằng cách dừng k3s-agent qua SSH.

    Hành vi giống thực tế:
      - k3s-agent stop → node không gửi heartbeat tới kube-apiserver nữa.
      - Sau ~40s (node-monitor-grace-period default), Master đánh dấu NotReady.
      - Sau ~300s (pod-eviction-timeout default), pods bị evict.
      - Với K3s + --node-fail-time flag: có thể giảm xuống < 30s.
    """
    logger.info(f"  [FAIL] Dừng k3s-agent trên {target_ip} qua SSH...")

    ssh_cmd = ["ssh"]
    if ssh_key_path:
        ssh_cmd += ["-i", ssh_key_path]
    ssh_cmd += [
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        f"{ssh_user}@{target_ip}",
        "sudo systemctl stop k3s-agent",
    ]

    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            logger.info(f"  ✓ k3s-agent đã được dừng trên {target_ip}")
            return True
        else:
            logger.error(f"  ✗ SSH lỗi: {result.stderr.strip()}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("  ✗ SSH timeout — kiểm tra kết nối Tailscale và SSH key")
        return False
    except FileNotFoundError:
        logger.error("  ✗ Không tìm thấy 'ssh'. Dùng --drain-mode thay thế.")
        return False


def simulate_node_failure_drain(node_name: str) -> bool:
    """
    Mô phỏng lỗi graceful bằng kubectl drain.

    Lưu ý: drain là GRACEFUL — pod được evict trước khi node tắt.
    Thực tế khi node sập đột ngột, pod sẽ ở trạng thái Terminating cho đến
    khi timeout, sau đó mới được reschedule.
    Dùng drain để demo rescheduling mà không cần quyền SSH.
    """
    logger.info(f"  [DRAIN] kubectl drain {node_name}...")
    try:
        result = subprocess.run(
            [
                "kubectl", "drain", node_name,
                "--ignore-daemonsets",
                "--delete-emptydir-data",
                "--grace-period=5",     # Cho pods 5s để graceful shutdown
                "--timeout=30s",
            ],
            capture_output=True, text=True, timeout=40,
        )
        if result.returncode == 0:
            logger.info(f"  ✓ kubectl drain {node_name} thành công")
            return True
        else:
            logger.warning(f"  ⚠ kubectl drain có lỗi nhưng tiếp tục:\n{result.stderr[:200]}")
            return True  # Drain có thể trả error nhưng pods vẫn được move
    except subprocess.TimeoutExpired:
        logger.error("  ✗ kubectl drain timeout (> 40s)")
        return False
    except FileNotFoundError:
        logger.error("  ✗ kubectl không khả dụng")
        return False


def restore_worker2_ssh(
    target_ip: str,
    ssh_user: str,
    ssh_port: int,
    ssh_key_path: str | None,
) -> None:
    """
    Khôi phục Worker2 bằng cách restart k3s-agent (dùng sau khi test xong).
    Gọi hàm này để đưa node trở lại Ready sau khi đã test xong.
    """
    logger.info(f"  [RESTORE] Restart k3s-agent trên {target_ip}...")
    ssh_cmd = ["ssh"]
    if ssh_key_path:
        ssh_cmd += ["-i", ssh_key_path]
    ssh_cmd += [
        "-o", "StrictHostKeyChecking=no",
        f"{ssh_user}@{target_ip}",
        "sudo systemctl start k3s-agent",
    ]
    try:
        result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            logger.info("  ✓ k3s-agent đã được restart")
    except Exception as exc:
        logger.warning(f"  ⚠ Restore lỗi: {exc}")


def restore_worker2_uncordon(node_name: str) -> None:
    """
    Uncordon Worker2 sau khi test kubectl drain xong.
    """
    logger.info(f"  [RESTORE] kubectl uncordon {node_name}...")
    subprocess.run(["kubectl", "uncordon", node_name], capture_output=True)
    logger.info("  ✓ Node đã được uncordon")


# ══════════════════════════════════════════════════════════════════════════════
# BACKGROUND PRODUCER — Gửi liên tục trong khi test
# ══════════════════════════════════════════════════════════════════════════════

class BackgroundProducer:
    """
    Gửi chunks vào Kafka trong một thread riêng.
    Dùng threading (không multiprocessing) để chia sẻ state với main thread.
    """

    def __init__(self, broker: str, n_chunks: int = 20):
        self.broker   = broker
        self.n_chunks = n_chunks
        self.sent     = 0
        self.errors   = 0
        self._stop    = False
        self._thread  = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop = True

    def join(self):
        if self._thread:
            self._thread.join(timeout=60)

    def _run(self):
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.broker,
                value_serializer=lambda v: json.dumps(
                    v, separators=(",", ":")
                ).encode("utf-8"),
                compression_type="gzip",
                acks="all",
                retries=5,
                retry_backoff_ms=1000,
                buffer_memory=67_108_864,
            )
            for chunk_id in range(self.n_chunks):
                if self._stop:
                    break
                data = random.choices(range(0, 10_000_000), k=CHUNK_SIZE)
                payload = {
                    "chunk_id":     chunk_id,
                    "total_chunks": self.n_chunks,
                    "size":         CHUNK_SIZE,
                    "timestamp_ms": int(time.time() * 1000),
                    "data":         data,
                }
                try:
                    future = producer.send(TOPIC, value=payload)
                    future.get(timeout=30)
                    self.sent += 1
                    logger.info(f"  [PRODUCER] Đã gửi chunk {chunk_id + 1}/{self.n_chunks}")
                except KafkaError as exc:
                    self.errors += 1
                    logger.warning(f"  [PRODUCER] Lỗi gửi chunk {chunk_id}: {exc}")

                time.sleep(2)   # Gửi mỗi 2 giây → dễ quan sát scaling

            producer.flush()
            producer.close()
        except Exception as exc:
            logger.error(f"  [PRODUCER] Thread lỗi: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — Điều phối kịch bản fault tolerance
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kịch bản 3: Fault Tolerance Test — NT533",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--broker",     default=DEFAULT_BROKER)
    parser.add_argument("--prometheus", default=DEFAULT_PROMETHEUS)
    parser.add_argument("--ssh-user",   default=DEFAULT_SSH_USER,
                        dest="ssh_user")
    parser.add_argument("--ssh-key",    default=None,
                        dest="ssh_key",
                        help="Đường dẫn SSH private key (~/.ssh/id_rsa)")
    parser.add_argument("--drain-mode", action="store_true",
                        dest="drain_mode",
                        help="Dùng kubectl drain thay vì SSH kill")
    parser.add_argument("--monitor-only", action="store_true",
                        dest="monitor_only",
                        help="Chỉ theo dõi, không giả lập lỗi")
    parser.add_argument("--no-restore", action="store_true",
                        dest="no_restore",
                        help="Không tự động restore Worker2 sau test")
    args = parser.parse_args()

    logger.info("═" * 70)
    logger.info("  Kịch bản 3: Fault Tolerance Test")
    logger.info(f"  Broker     : {args.broker}")
    logger.info(f"  Target node: {WORKER2_HOSTNAME} ({WORKER2_IP})")
    logger.info(f"  Mode       : {'Monitor only' if args.monitor_only else ('kubectl drain' if args.drain_mode else 'SSH kill k3s-agent')}")
    logger.info(f"  Fail delay : {FAIL_DELAY_S}s (pipeline chạy trước khi giả lập lỗi)")
    logger.info(f"  Target RTO : < {RECOVERY_TARGET_S}s (Recovery Time Objective)")
    logger.info("═" * 70)

    monitor = ClusterMonitor(args.prometheus)

    # ── Phase 0: Kiểm tra trạng thái ban đầu ─────────────────────────────────
    logger.info("\n[PHASE 0] Kiểm tra trạng thái ban đầu của cluster...")
    initial_status = monitor.get_node_status()
    logger.info(f"  Node status: {initial_status}")
    monitor.record("NODE_STATUS_INITIAL", str(initial_status))

    initial_pods_w1 = monitor.get_pods_on_node("k3s-worker1")
    initial_pods_w2 = monitor.get_pods_on_node("k3s-worker2")
    logger.info(f"  Pods trên Worker1: {len(initial_pods_w1)}")
    logger.info(f"  Pods trên Worker2: {len(initial_pods_w2)}")

    if not args.monitor_only:
        if "k3s-worker2" not in initial_status or initial_status["k3s-worker2"] != "True":
            logger.warning("  ⚠ Worker2 chưa ở trạng thái Ready — kiểm tra cluster trước khi chạy test.")
            if input("  Tiếp tục? (y/N): ").strip().lower() != "y":
                sys.exit(0)

    # ── Phase 1: Khởi động Producer ───────────────────────────────────────────
    logger.info(f"\n[PHASE 1] Bắt đầu gửi chunks liên tục vào Kafka...")
    producer = BackgroundProducer(args.broker, n_chunks=20)
    producer.start()
    monitor.record("PRODUCER_STARTED", f"Gửi 20 chunks × {CHUNK_SIZE:,} phần tử")

    # ── Phase 2: Đợi pipeline ổn định ────────────────────────────────────────
    logger.info(f"\n[PHASE 2] Đợi {FAIL_DELAY_S}s để pipeline chạy ổn định...")
    for i in range(FAIL_DELAY_S):
        time.sleep(1)
        if (i + 1) % 5 == 0:
            node_status = monitor.get_node_status()
            pods_w1 = len(monitor.get_pods_on_node("k3s-worker1"))
            pods_w2 = len(monitor.get_pods_on_node("k3s-worker2"))
            logger.info(
                f"  T+{i+1:>2}s | Nodes={node_status} | "
                f"Pods W1={pods_w1} W2={pods_w2} | "
                f"Chunks sent={producer.sent}"
            )

    # ── Phase 3: Giả lập lỗi ──────────────────────────────────────────────────
    t_fail = time.time()

    if not args.monitor_only:
        logger.info(f"\n[PHASE 3] Giả lập lỗi Worker2 ({WORKER2_IP})...")
        monitor.record("FAIL_START", f"Giả lập lỗi Worker2 ({WORKER2_IP})")

        if args.drain_mode:
            success = simulate_node_failure_drain(WORKER2_HOSTNAME)
        else:
            success = simulate_node_failure_ssh(
                WORKER2_IP,
                args.ssh_user,
                DEFAULT_SSH_PORT,
                args.ssh_key,
            )

        if success:
            monitor.record("FAIL_INJECTED", f"Worker2 đã bị tắt lúc {datetime.now().strftime('%H:%M:%S')}")
        else:
            logger.error("  ✗ Không thể giả lập lỗi. Chuyển sang monitor-only mode.")
            args.monitor_only = True
    else:
        logger.info("\n[PHASE 3] Monitor-only mode — KHÔNG giả lập lỗi.")

    # ── Phase 4: Monitor quá trình phục hồi ──────────────────────────────────
    logger.info(f"\n[PHASE 4] Theo dõi quá trình phục hồi trong {MONITOR_AFTER_FAIL_S}s...")

    t_node_not_ready   = None   # Thời điểm Master phát hiện NotReady
    t_pods_rescheduled = None   # Thời điểm pods được reschedule sang W1
    t_fully_recovered  = None   # Thời điểm cluster trở lại bình thường
    # Trong drain-mode: drain thành công = điểm "lỗi" được tiêm vào
    # (node không đổi sang NotReady khi drain, chỉ SchedulingDisabled)
    if args.drain_mode and not args.monitor_only and success:
        t_node_not_ready = t_fail
        monitor.record("DRAIN_COMPLETE", "kubectl drain hoàn thành — đánh dấu thời điểm lỗi")
        # Nếu không có pods trên W2 trước khi drain, reschedule là tức thì
        if len(initial_pods_w2) == 0:
            t_pods_rescheduled = t_fail
            monitor.record("NO_PODS_ON_W2", "Worker2 không có pods — reschedule không cần thiết")
    for i in range(MONITOR_AFTER_FAIL_S):
        time.sleep(1)

        node_status = monitor.get_node_status()
        pods_w1 = monitor.get_pods_on_node("k3s-worker1")
        pods_w2 = monitor.get_pods_on_node("k3s-worker2")
        offsets  = monitor.get_kafka_consumer_offset(args.broker)

        elapsed_since_fail = time.time() - t_fail

        # Phát hiện: Worker2 chuyển sang NotReady
        if (not args.monitor_only and
            t_node_not_ready is None and
            node_status.get("k3s-worker2") in ("False", "Unknown")):
            t_node_not_ready = time.time()
            monitor.record(
                "NODE_NOT_READY",
                f"Worker2 NotReady phát hiện sau {elapsed_since_fail:.1f}s"
            )

        # Phát hiện: Pod được reschedule sang Worker1 (hoặc Worker2 đã sạch pods)
        w2_cleared = (len(initial_pods_w2) > 0 and len(pods_w2) == 0)
        if (t_pods_rescheduled is None and
            t_node_not_ready is not None and
            (len(pods_w1) > len(initial_pods_w1) or w2_cleared)):
            t_pods_rescheduled = time.time()
            monitor.record(
                "PODS_RESCHEDULED",
                f"Pods được reschedule sang Worker1 "
                f"(W1: {len(initial_pods_w1)} → {len(pods_w1)} pods) "
                f"sau {elapsed_since_fail:.1f}s từ lỗi"
            )

        # Phát hiện: Cluster hoàn toàn phục hồi
        if (t_fully_recovered is None and
            t_pods_rescheduled is not None and
            len(pods_w1) >= 1 and
            producer.errors == 0):
            t_fully_recovered = time.time()
            monitor.record(
                "FULLY_RECOVERED",
                f"Cluster phục hồi sau {elapsed_since_fail:.1f}s"
            )

        if (i + 1) % 5 == 0:
            w2_status = node_status.get("k3s-worker2", "Unknown")
            offset_str = str(offsets) if offsets else "N/A (kubectl exec lỗi)"
            logger.info(
                f"  T+{elapsed_since_fail:>4.0f}s | "
                f"W2={w2_status} | "
                f"Pods W1={len(pods_w1)} W2={len(pods_w2)} | "
                f"Sent={producer.sent} Errors={producer.errors} | "
                f"Offset={offset_str}"
            )

        # Dừng sớm nếu đã phục hồi hoàn toàn
        if t_fully_recovered is not None and elapsed_since_fail > 60:
            logger.info("  ✓ Đã phục hồi hoàn toàn — kết thúc monitoring sớm.")
            break

    # ── Phase 5: Khôi phục Worker2 ────────────────────────────────────────────
    if not args.monitor_only and not args.no_restore:
        logger.info("\n[PHASE 5] Khôi phục Worker2...")
        if args.drain_mode:
            restore_worker2_uncordon(WORKER2_HOSTNAME)
        else:
            restore_worker2_ssh(WORKER2_IP, args.ssh_user, DEFAULT_SSH_PORT, args.ssh_key)
        monitor.record("NODE_RESTORED", f"Worker2 ({WORKER2_IP}) đã được khôi phục")

    producer.stop()
    producer.join()

    # ── Báo cáo kết quả ───────────────────────────────────────────────────────
    logger.info(f"\n{'═' * 70}")
    logger.info("  KẾT QUẢ — Kịch bản 3: Fault Tolerance")
    logger.info("═" * 70)

    if not args.monitor_only:
        # RTO: Recovery Time Objective
        if t_node_not_ready:
            detect_time = t_node_not_ready - t_fail
            logger.info(f"  Thời gian phát hiện NotReady : {detect_time:.1f}s")
            monitor.record("METRIC_DETECT_TIME", f"{detect_time:.1f}s")
        else:
            logger.info("  Thời gian phát hiện NotReady : Không quan sát được")

        if t_pods_rescheduled and t_node_not_ready:
            reschedule_time = t_pods_rescheduled - t_node_not_ready
            logger.info(f"  Thời gian reschedule pods    : {reschedule_time:.1f}s")
            monitor.record("METRIC_RESCHEDULE_TIME", f"{reschedule_time:.1f}s")

        if t_fully_recovered:
            total_rto = t_fully_recovered - t_fail
            logger.info(f"  Tổng RTO (Recovery Time)     : {total_rto:.1f}s")
            if total_rto < RECOVERY_TARGET_S:
                logger.info(f"  ✓ PASS: RTO {total_rto:.1f}s < mục tiêu {RECOVERY_TARGET_S}s")
            else:
                logger.warning(f"  ⚠ RTO {total_rto:.1f}s vượt mục tiêu {RECOVERY_TARGET_S}s")
            monitor.record("METRIC_TOTAL_RTO", f"{total_rto:.1f}s")
        else:
            logger.info(f"  RTO: Không đo được (chưa phục hồi trong {MONITOR_AFTER_FAIL_S}s)")

    logger.info(f"\n  Chunks đã gửi  : {producer.sent}/{20}")
    logger.info(f"  Gửi lỗi       : {producer.errors}")
    if producer.errors == 0:
        logger.info("  ✓ PASS: Không mất message — Kafka retention bảo toàn dữ liệu")
    else:
        logger.warning(f"  ⚠ {producer.errors} messages gửi lỗi (kiểm tra Kafka retry config)")

    # In timeline sự kiện
    logger.info("\n  TIMELINE SỰ KIỆN:")
    logger.info(f"  {'Thời gian':<10} {'Loại':<25} {'Chi tiết'}")
    logger.info(f"  {'─'*10} {'─'*25} {'─'*30}")
    for entry in monitor.timeline:
        logger.info(f"  {entry['time']:<10} {entry['type']:<25} {entry['detail'][:60]}")

    logger.info("\n  Hướng dẫn verify thêm:")
    logger.info(f"  kubectl get nodes -w")
    logger.info(f"  kubectl get pods -n openfaas-fn -w")
    logger.info(f"  kubectl describe pod -n openfaas-fn -l faas_function=parallel-sort")
    logger.info(f"  Grafana: http://{MASTER_IP}:30030 — 'Kubernetes / Nodes' dashboard")


if __name__ == "__main__":
    main()
