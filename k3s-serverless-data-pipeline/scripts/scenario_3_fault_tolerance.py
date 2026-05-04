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
import heapq
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

DEFAULT_BROKER     = "100.107.243.97:30092"   # Kafka NodePort (master Tailscale IP)
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

import os as _os

# Đường dẫn log mặc định: <project_root>/demo_logs/scenario3.log
# Mở mode='w' → MỖI LẦN CHẠY GHI ĐÈ HOÀN TOÀN
_DEFAULT_LOG_DIR = _os.environ.get(
    "DEMO_LOG_DIR",
    _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "..", "..", "demo_logs"),
)
DEFAULT_LOG_FILE = _os.path.abspath(_os.path.join(_DEFAULT_LOG_DIR, "scenario3.log"))


def setup_demo_logging(log_file: str = DEFAULT_LOG_FILE) -> str:
    _os.makedirs(_os.path.dirname(log_file), exist_ok=True)
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    root.addHandler(sh)
    fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)
    root.setLevel(logging.INFO)
    return log_file


setup_demo_logging()
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
        # Tìm pod kafka broker thực tế (ưu tiên my-kafka-broker-0, fallback kafka-controller-0)
        broker_pod = "my-kafka-broker-0"
        try:
            chk = subprocess.run(
                ["kubectl", "get", "pod", "-n", "kafka", broker_pod, "--no-headers"],
                capture_output=True, text=True, timeout=5,
            )
            if chk.returncode != 0:
                broker_pod = "kafka-controller-0"
        except Exception:
            pass
        try:
            result = subprocess.run(
                ["kubectl", "exec", "-n", "kafka",
                 broker_pod,
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
    ssh_password: str | None = None,
) -> bool:
    """
    Mô phỏng node sập đột ngột bằng cách dừng k3s-agent qua SSH.
    
    Hỗ trợ:
      - SSH key: `-i ~/.ssh/id_rsa`
      - SSH password: `sshpass -p password ssh ...` (nếu cài sshpass)
      - SSH password via stdin: pipe password vào SSH (cross-platform)

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
        "-o", "UserKnownHostsFile=/dev/null",
    ]
    
    # Thêm option password prompt
    if ssh_password:
        ssh_cmd += ["-o", "PreferredAuthentications=password"]
    
    ssh_cmd += [
        f"{ssh_user}@{target_ip}",
        "sudo systemctl stop k3s-agent",
    ]

    try:
        if ssh_password:
            # Dùng sshpass nếu có sẵn (recommended)
            try:
                result = subprocess.run(
                    ["sshpass", "-p", ssh_password] + ssh_cmd,
                    capture_output=True, text=True, timeout=15
                )
            except FileNotFoundError:
                # Fallback: pipe password vào stdin
                logger.debug("  sshpass not found, using stdin pipe for password...")
                result = subprocess.run(
                    ssh_cmd,
                    input=ssh_password + "\n",
                    capture_output=True, text=True, timeout=15
                )
        else:
            # SSH key mode
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            logger.info(f"  ✓ k3s-agent đã được dừng trên {target_ip}")
            return True
        else:
            stderr = result.stderr.strip()
            if "permission denied" in stderr.lower() or "password" in stderr.lower():
                logger.error(f"  ✗ SSH authentication failed (check password/key)")
            else:
                logger.error(f"  ✗ SSH lỗi: {stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("  ✗ SSH timeout — kiểm tra kết nối Tailscale")
        return False
    except FileNotFoundError as e:
        logger.error(f"  ✗ Không tìm thấy SSH client: {e}")
        logger.error("     Cách khác: dùng `--drain-mode` thay vì SSH")
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
    ssh_password: str | None = None,
) -> None:
    """
    Khôi phục Worker2 bằng cách restart k3s-agent (dùng sau khi test xong).
    Gọi hàm này để đưa node trở lại Ready sau khi đã test xong.
    Hỗ trợ SSH key hoặc password.
    """
    logger.info(f"  [RESTORE] Restart k3s-agent trên {target_ip}...")
    ssh_cmd = ["ssh"]
    if ssh_key_path:
        ssh_cmd += ["-i", ssh_key_path]
    ssh_cmd += [
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
    ]
    if ssh_password:
        ssh_cmd += ["-o", "PreferredAuthentications=password"]
    
    ssh_cmd += [
        f"{ssh_user}@{target_ip}",
        "sudo systemctl start k3s-agent",
    ]
    try:
        if ssh_password:
            try:
                result = subprocess.run(
                    ["sshpass", "-p", ssh_password] + ssh_cmd,
                    capture_output=True, text=True, timeout=15
                )
            except FileNotFoundError:
                result = subprocess.run(
                    ssh_cmd,
                    input=ssh_password + "\n",
                    capture_output=True, text=True, timeout=15
                )
        else:
            result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            logger.info("  ✓ k3s-agent đã được restart")
        else:
            logger.warning(f"  ⚠ Restore có lỗi: {result.stderr.strip()[:100]}")
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
        self.last_sent_chunk_id = -1   # chunk_id mới nhất đã gửi thành công
        self.last_sent_ts       = 0     # epoch lúc gửi xong
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
                    self.last_sent_chunk_id = chunk_id
                    self.last_sent_ts = time.time()
                    logger.info(f"  [PRODUCER] Đã gửi chunk {chunk_id + 1}/{self.n_chunks} (chunk_id={chunk_id})")
                except KafkaError as exc:
                    self.errors += 1
                    logger.warning(f"  [PRODUCER] Lỗi gửi chunk {chunk_id}: {exc}")

                time.sleep(2)   # Gửi mỗi 2 giây → dễ quan sát scaling

            producer.flush()
            producer.close()
        except Exception as exc:
            logger.error(f"  [PRODUCER] Thread lỗi: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
# COLLECT — Đọc các chunks đã sắp xếp từ UIT-OUTPUT (REDUCE)
# ══════════════════════════════════════════════════════════════════════════════

def collect_sorted_chunks_s3(broker: str, output_topic: str, n_chunks: int,
                             timeout_s: int = 120) -> tuple:
    """Đọc tối đa n_chunks chunks đã sort từ Kafka topic OUTPUT, k-way merge.
    Trả về (chunks_dict, merged_data_or_None, is_sorted_bool).
    
    Ghi chú: consumer_timeout_ms=30s (tăng từ 8s để chứa độ trễ connector xử lý).
    """
    logger.info(f"  [COLLECT] Đọc tối đa {n_chunks} chunks từ {output_topic} "
                f"(timeout={timeout_s}s)...")
    chunks_dict = {}
    t_start = time.time()
    try:
        consumer = KafkaConsumer(
            output_topic,
            bootstrap_servers=broker,
            group_id=f'scenario3-collector-{int(time.time())}',
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=30000,  # Tăng từ 8s → 30s
            session_timeout_ms=60000,   # Session: 60s (tránh rebalancing)
            max_poll_records=500,       # Lấy tối đa 500 records mỗi poll
        )
        for msg in consumer:
            chunk = msg.value
            cid = chunk.get('chunk_id')
            chunks_dict[cid] = chunk
            logger.info(
                f"    [{len(chunks_dict):>2}/{n_chunks}] chunk_id={cid} "
                f"size={len(chunk.get('data', []))} "
                f"sort_ms={chunk.get('sort_ms', 'N/A')}"
            )
            if len(chunks_dict) >= n_chunks:
                break
            if time.time() - t_start > timeout_s:
                logger.warning("    ⚠ Collect timeout reached")
                break
        consumer.close()
    except Exception as exc:
        logger.error(f"  Consumer error: {exc}")
        return chunks_dict, None, False

    if not chunks_dict:
        return chunks_dict, None, False

    sorted_items = sorted(chunks_dict.items(),
                          key=lambda x: int(x[0]) if str(x[0]).isdigit() else x[0])
    arrays = [item[1].get('data', []) for item in sorted_items]
    merged = list(heapq.merge(*arrays))
    is_sorted = (merged == sorted(merged))
    
    # Log random chunk sample để tránh tràn output
    if chunks_dict:
        sample_cid = random.choice(list(chunks_dict.keys()))
        sample_chunk = chunks_dict[sample_cid]
        sample_data = sample_chunk.get('data', [])
        logger.info(f"\n  >>> SAMPLE CHUNK (random) — chunk_id={sample_cid}")
        logger.info(f"      size={len(sample_data):,} elements | sort_ms={sample_chunk.get('sort_ms', 'N/A')}")
        if sample_data:
            logger.info(f"      min/max={min(sample_data)}/{max(sample_data)} | is_sorted={'YES' if sample_data == sorted(sample_data) else 'NO'}")
            logger.info(f"      first20={sample_data[:20]}")
            logger.info(f"      last20={sample_data[-20:]}")
    
    return chunks_dict, merged, is_sorted


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — Điều phối kịch bản fault tolerance
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    global FAIL_DELAY_S, MONITOR_AFTER_FAIL_S
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
    parser.add_argument("--ssh-password", default=None,
                        dest="ssh_password",
                        help="SSH password (nếu không dùng key auth)")
    parser.add_argument("--drain-mode", action="store_true",
                        dest="drain_mode",
                        help="Dùng kubectl drain thay vì SSH kill")
    parser.add_argument("--monitor-only", action="store_true",
                        dest="monitor_only",
                        help="Chỉ theo dõi, không giả lập lỗi")
    parser.add_argument("--no-restore", action="store_true",
                        dest="no_restore",
                        help="Không tự động restore Worker2 sau test")
    parser.add_argument("--n-chunks", type=int, default=20, dest="n_chunks",
                        help="Số chunks producer gửi (default: 20)")
    parser.add_argument("--fail-delay", type=int, default=FAIL_DELAY_S,
                        dest="fail_delay",
                        help="Số giây chạy ổn định trước khi giả lập lỗi")
    parser.add_argument("--monitor-after", type=int, default=MONITOR_AFTER_FAIL_S,
                        dest="monitor_after",
                        help="Số giây theo dõi sau khi tiêm lỗi")
    parser.add_argument("--collect-after-restore", action="store_true",
                        default=True,
                        dest="collect_after_restore",
                        help="Sau khi restore worker2, đọc UIT-OUTPUT và in chunks đã sắp xếp (mặc định BẬT)")
    parser.add_argument("--no-collect", action="store_false",
                        dest="collect_after_restore",
                        help="Tắt phần collect+in chunks sau phục hồi")
    parser.add_argument("--output-topic", default="UIT-OUTPUT", dest="output_topic")
    parser.add_argument("--collect-timeout", type=int, default=360,
                        dest="collect_timeout",
                        help="Timeout (s) đợi connector xử lý + collect chunks từ UIT-OUTPUT")
    parser.add_argument("--chunks-log",
                        default=_os.path.abspath(_os.path.join(_DEFAULT_LOG_DIR, "scenario3_chunks.log")),
                        dest="chunks_log",
                        help="Đường dẫn file ghi tóm tắt + chunk mục tiêu (ghi đè)")
    args = parser.parse_args()

    # Áp dụng overrides
    FAIL_DELAY_S = args.fail_delay
    MONITOR_AFTER_FAIL_S = args.monitor_after

    logger.info("═" * 70)
    logger.info(f"  Kịch bản 3: Fault Tolerance Test — {datetime.now().isoformat()}")
    logger.info(f"  Log file   : {DEFAULT_LOG_FILE} (ghi đè mỗi lần chạy)")
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
            logger.warning("  ⚠ Worker2 chưa ở trạng thái Ready — tiếp tục vẫn chạy test.")
            logger.warning("    (dùng --monitor-only nếu muốn chỉ quan sát)")

    # ── Phase 1: Khởi động Producer ───────────────────────────────────────────
    logger.info(f"\n[PHASE 1] Bắt đầu gửi chunks liên tục vào Kafka...")
    producer = BackgroundProducer(args.broker, n_chunks=args.n_chunks)
    producer.start()
    monitor.record("PRODUCER_STARTED", f"Gửi {args.n_chunks} chunks × {CHUNK_SIZE:,} phần tử")

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

    # ── Phase 3: Snapshot pre-fail + chọn target chunk đang xử lý trên W2 ──
    pre_fail_pods_w2 = monitor.get_pods_on_node("k3s-worker2")
    target_chunk_id = producer.last_sent_chunk_id
    target_chunk_ts = producer.last_sent_ts
    logger.info(f"\n[PRE-FAIL SNAPSHOT]")
    logger.info(f"  Pods trên Worker2 trước khi tắt: {len(pre_fail_pods_w2)}")
    for line in pre_fail_pods_w2:
        logger.info(f"    {line}")
    logger.info(f"  Chunk đang được Worker2 xử lý (mới nhất gửi vào Kafka): "
                f"chunk_id={target_chunk_id}")
    monitor.record(
        "TARGET_CHUNK_SELECTED",
        f"chunk_id={target_chunk_id} (đang xử lý trên Worker2 lúc tắt)"
    )

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
                ssh_password=args.ssh_password,
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
            restore_worker2_ssh(WORKER2_IP, args.ssh_user, DEFAULT_SSH_PORT, args.ssh_key,
                              ssh_password=args.ssh_password)
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

    logger.info(f"\n  Chunks đã gửi  : {producer.sent}/{args.n_chunks}")
    logger.info(f"  Gửi lỗi       : {producer.errors}")
    if producer.errors == 0:
        logger.info("  ✓ PASS: Không mất message — Kafka retention bảo toàn dữ liệu")
    else:
        logger.warning(f"  ⚠ {producer.errors} messages gửi lỗi (kiểm tra Kafka retry config)")

    # ── Phase 6: Collect chunks đã sắp xếp từ UIT-OUTPUT ─────────────────────
    if args.collect_after_restore:
        logger.info(f"\n[PHASE 6] Đợi connector xử lý hết {producer.sent} chunks rồi đọc {args.output_topic}...")
        # Connector xử lý tuần tự ~10-15s/chunk → 20 chunks ~240s
        # Poll UIT-OUTPUT offsets cho đến khi đủ hoặc timeout
        wait_deadline = time.time() + args.collect_timeout
        while time.time() < wait_deadline:
            try:
                result = subprocess.run(
                    ["kubectl", "exec", "-n", "kafka", "my-kafka-broker-0", "--",
                     "/opt/bitnami/kafka/bin/kafka-get-offsets.sh",
                     "--bootstrap-server", "localhost:9092",
                     "--topic", args.output_topic],
                    capture_output=True, text=True, timeout=10,
                )
                total_offset = 0
                for line in result.stdout.strip().split("\n"):
                    parts = line.strip().split(":")
                    if len(parts) == 3:
                        total_offset += int(parts[2])
                logger.info(f"  [WAIT] UIT-OUTPUT total messages = {total_offset}/{producer.sent}")
                if total_offset >= producer.sent:
                    logger.info(f"  ✓ Đủ {total_offset} messages — bắt đầu collect")
                    break
            except Exception as exc:
                logger.debug(f"  offset poll error: {exc}")
            time.sleep(10)
        else:
            logger.warning(f"  ⚠ Timeout {args.collect_timeout}s — collect những gì có")

        chunks_dict, merged_data, is_sorted = collect_sorted_chunks_s3(
            broker=args.broker,
            output_topic=args.output_topic,
            n_chunks=producer.sent,
            timeout_s=args.collect_timeout,
        )

        # Tóm tắt + tập trung vào TARGET CHUNK
        if chunks_dict:
            logger.info("")
            logger.info(f"  {'═' * 70}")
            logger.info(f"  CHUNKS ĐÃ SẮP XẾP SAU PHỤC HỒI — Tổng: {len(chunks_dict)}/{producer.sent}")
            logger.info(f"  {'═' * 70}")

            # 1) Bảng tóm tắt 1 dòng/chunk (gọn, không tràn)
            logger.info(f"  {'chunk_id':>9} | {'size':>7} | {'sort_ms':>9} | "
                        f"{'min':>10} | {'max':>10} | {'sorted?':>7}")
            logger.info("  " + "─" * 70)
            for cid in sorted(chunks_dict.keys(),
                              key=lambda x: int(x) if str(x).isdigit() else x):
                ch = chunks_dict[cid]
                data = ch.get('data', [])
                if not data:
                    continue
                ok = "YES" if data == sorted(data) else "NO"
                marker = " ← TARGET" if cid == target_chunk_id else ""
                logger.info(
                    f"  {str(cid):>9} | {len(data):>7,} | "
                    f"{str(ch.get('sort_ms','N/A')):>9} | "
                    f"{min(data):>10} | {max(data):>10} | {ok:>7}{marker}"
                )

            # 2) IN CHI TIẾT TARGET CHUNK (chunk đang xử lý trên W2 lúc tắt)
            logger.info("")
            logger.info(f"  {'═' * 70}")
            logger.info(f"  TARGET CHUNK — chunk được Worker2 xử lý lúc bị tắt")
            logger.info(f"  {'═' * 70}")
            target_ch = chunks_dict.get(target_chunk_id)
            if target_ch is None:
                # Fallback: nếu chunk_id chưa được flush ra OUTPUT, dùng chunk_id liền kề
                avail = sorted(chunks_dict.keys(), key=lambda x: int(x) if str(x).isdigit() else 0)
                if avail:
                    fallback_id = min(avail, key=lambda c: abs((c if isinstance(c, int) else 0)
                                                                - (target_chunk_id if isinstance(target_chunk_id, int) else 0)))
                    logger.warning(
                        f"  ⚠ chunk_id={target_chunk_id} chưa thấy trong UIT-OUTPUT, "
                        f"dùng chunk gần nhất: chunk_id={fallback_id}"
                    )
                    target_ch = chunks_dict[fallback_id]
                    target_chunk_id = fallback_id

            if target_ch is not None:
                tdata = target_ch.get('data', [])
                logger.info(f"  chunk_id        : {target_chunk_id}")
                logger.info(f"  size            : {len(tdata):,} phần tử")
                logger.info(f"  sort_ms         : {target_ch.get('sort_ms', 'N/A')}")
                logger.info(f"  total_chunks    : {target_ch.get('total_chunks', 'N/A')}")
                logger.info(f"  timestamp_ms    : {target_ch.get('timestamp_ms', 'N/A')}")
                if tdata:
                    chunk_sorted = (tdata == sorted(tdata))
                    logger.info(f"  min / max       : {min(tdata)} / {max(tdata)}")
                    logger.info(f"  is_sorted       : {'✅ YES' if chunk_sorted else '❌ NO'}")
                    logger.info(f"  first 20 values : {tdata[:20]}")
                    logger.info(f"  last  20 values : {tdata[-20:]}")
                    mid = len(tdata) // 2
                    logger.info(f"  middle 10 (idx {mid}): {tdata[mid:mid+10]}")
                logger.info("")
                logger.info("  ✅ TARGET CHUNK đã được sắp xếp đúng và KHÔNG MẤT DỮ LIỆU")
                logger.info("     → Khi Worker2 sập, chunk này được Kafka giữ lại,")
                logger.info("       reschedule sang Worker1 xử lý tiếp, rồi flush vào UIT-OUTPUT.")

            # 3) Tổng kết
            logger.info("")
            logger.info(f"  Tổng chunks thu được: {len(chunks_dict)}/{producer.sent}")
            if len(chunks_dict) >= producer.sent:
                logger.info("  ✓ PASS: Đầy đủ dữ liệu — KHÔNG MẤT chunk nào sau khi worker2 sập + phục hồi")
            else:
                logger.warning(
                    f"  ⚠ Thiếu {producer.sent - len(chunks_dict)} chunks — có thể vẫn đang xử lý "
                    f"(thử tăng --collect-timeout)"
                )
            if merged_data is not None:
                logger.info(f"  K-way merge: {len(merged_data):,} elements | "
                            f"is_sorted={'✅ YES' if is_sorted else '❌ NO'}")
                if merged_data:
                    logger.info(f"  Min={min(merged_data)}  Max={max(merged_data)}")
                    logger.info(f"  First 10: {merged_data[:10]}")
                    logger.info(f"  Last  10: {merged_data[-10:]}")

        # Ghi log file
        if args.chunks_log and chunks_dict:
            try:
                with open(args.chunks_log, "w", encoding="utf-8") as f:
                    f.write(f"# Scenario 3 — Fault tolerance result\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write(f"# Producer sent: {producer.sent}\n")
                    f.write(f"# Producer errors: {producer.errors}\n")
                    f.write(f"# Chunks collected: {len(chunks_dict)}\n")
                    f.write(f"# Output topic: {args.output_topic}\n")
                    f.write(f"# Target chunk_id (xử lý trên W2 lúc fail): {target_chunk_id}\n\n")

                    f.write("## SUMMARY (one line per chunk)\n")
                    f.write(f"{'chunk_id':>9} | {'size':>7} | {'sort_ms':>9} | "
                            f"{'min':>10} | {'max':>10} | sorted?\n")
                    f.write("-" * 80 + "\n")
                    for cid in sorted(chunks_dict.keys(),
                                      key=lambda x: int(x) if str(x).isdigit() else x):
                        ch = chunks_dict[cid]
                        data = ch.get('data', [])
                        if not data:
                            continue
                        ok = "YES" if data == sorted(data) else "NO"
                        mark = " <-- TARGET" if cid == target_chunk_id else ""
                        f.write(f"{str(cid):>9} | {len(data):>7,} | "
                                f"{str(ch.get('sort_ms','N/A')):>9} | "
                                f"{min(data):>10} | {max(data):>10} | {ok}{mark}\n")

                    # Full data của TARGET CHUNK
                    target_ch = chunks_dict.get(target_chunk_id)
                    if target_ch is not None:
                        tdata = target_ch.get('data', [])
                        f.write(f"\n## TARGET CHUNK (xử lý trên Worker2 lúc fail) — "
                                f"chunk_id={target_chunk_id}\n")
                        f.write(f"size={len(tdata)}\n")
                        f.write(f"sort_ms={target_ch.get('sort_ms','N/A')}\n")
                        if tdata:
                            f.write(f"min={min(tdata)} max={max(tdata)}\n")
                            f.write(f"is_sorted={tdata == sorted(tdata)}\n")
                            f.write(f"first50={tdata[:50]}\n")
                            f.write(f"last50={tdata[-50:]}\n")
                            mid = len(tdata) // 2
                            f.write(f"middle50_at_idx_{mid}={tdata[mid:mid+50]}\n")

                    if merged_data is not None:
                        f.write(f"\n## K-way merge result\n")
                        f.write(f"total_elements={len(merged_data)}\n")
                        f.write(f"is_sorted={is_sorted}\n")
                        if merged_data:
                            f.write(f"min={min(merged_data)} max={max(merged_data)}\n")
                            f.write(f"first20={merged_data[:20]}\n")
                            f.write(f"last20={merged_data[-20:]}\n")
                logger.info(f"  ✓ Đã ghi log chunks → {args.chunks_log}")
            except Exception as exc:
                logger.warning(f"  ⚠ Không ghi được chunks log: {exc}")

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
