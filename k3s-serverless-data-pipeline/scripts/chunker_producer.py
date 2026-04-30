#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  chunker_producer.py — Lớp Ingestion (Data Ingestion Layer)                ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Nhiệm vụ:
  - Tạo mảng 1.000.000 số nguyên ngẫu nhiên.
  - Áp dụng kỹ thuật CHUNKING: cắt thành các khối 50.000 phần tử (~10MB JSON)
    để tránh serialize một JSON khổng lồ làm sập RAM Producer.
  - Đẩy từng chunk vào Kafka topic 'UIT' dưới dạng JSON payload.

Yêu cầu:
  pip install kafka-python>=2.0.2

Cách chạy:
  # Trong cluster (từ một pod):
  python chunker_producer.py

  # Ngoài cluster qua Tailscale (NodePort trên Master node k3s-master):
  python chunker_producer.py --broker 192.168.125.104:30092
"""

import json
import time
import random
import argparse
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Hằng số cấu hình ─────────────────────────────────────────────────────────

# [NETWORKING] Địa chỉ Kafka broker trong K3s cluster:
#   - Pod-to-pod (trong cluster): "kafka.kafka.svc.cluster.local:9092"
#     → K3s CoreDNS phân giải tên service → ClusterIP → broker pod
#   - Từ máy ngoài qua LAN NodePort:
#       Master  (k3s-master)  : 192.168.125.104:30092
#       Worker1 (k3s-worker1) : 192.168.125.105:30092
#       Worker2 (k3s-worker2) : 192.168.125.106:30092
#   - Giao thức PLAINTEXT (không TLS) — chỉ dùng trong mạng LAN nội bộ.
DEFAULT_BROKER = "192.168.125.104:30092"  # Kafka NodePort (tests run on master OS)

TOPIC_NAME     = "UIT"            # Topic đích — phải tồn tại hoặc auto-create bật
TOTAL_ELEMENTS = 1_000_000        # Tổng số phần tử cần sinh và gửi
CHUNK_SIZE     = 50_000           # Kích thước mỗi chunk: 50K int ≈ 400KB JSON raw
                                  # ≈ 100KB sau gzip → không vượt message.max.bytes
VALUE_MIN      = 0
VALUE_MAX      = 10_000_000       # Khoảng giá trị [0, 10_000_000)


# ─── Hàm tạo dữ liệu ──────────────────────────────────────────────────────────

def generate_data(n: int) -> list:
    """
    Tạo danh sách n số nguyên ngẫu nhiên.

    [MEMORY] random.choices() với range object:
      - range(VALUE_MAX) chỉ chiếm O(1) bộ nhớ (không tạo list).
      - Kết quả list[int]: n × ~28 bytes/object ≈ 28MB cho 1M phần tử.
      - Đây là mức chấp nhận được; tránh random.sample() vì nó tạo
        thêm một tập hợp nội bộ ~10M phần tử để đảm bảo unique.
    """
    logger.info(f"Đang tạo {n:,} số nguyên ngẫu nhiên trong [{VALUE_MIN}, {VALUE_MAX})...")
    t0 = time.perf_counter()
    data = random.choices(range(VALUE_MIN, VALUE_MAX), k=n)
    elapsed = time.perf_counter() - t0
    logger.info(
        f"✓ Tạo xong trong {elapsed:.2f}s | "
        f"Bộ nhớ ước tính: ~{n * 28 / 1024**2:.1f} MB"
    )
    return data


# ─── Hàm khởi tạo KafkaProducer ───────────────────────────────────────────────

def create_producer(broker: str) -> KafkaProducer:
    """
    Khởi tạo KafkaProducer tối ưu cho throughput cao.

    [NETWORKING] Kết nối PLAINTEXT:
      - Không cần ssl_context hay sasl_mechanism vì mạng Tailscale
        đã cung cấp mã hóa end-to-end WireGuard ở tầng IP.
      - Trong production (public internet): BẮT BUỘC dùng TLS + SASL.

    [MEMORY] buffer_memory = 64MB:
      - Tổng RAM Producer dùng để buffer các record chờ gửi.
      - Nếu buffer đầy và linger_ms chưa trôi qua, send() sẽ block.
      - 64MB >> kích thước một chunk nén (~100KB) → không bao giờ block.
    """
    logger.info(f"Kết nối tới Kafka broker: {broker}")
    return KafkaProducer(
        bootstrap_servers=broker,

        # Serialize Python dict → JSON bytes trước khi gửi
        value_serializer=lambda v: json.dumps(
            v, separators=(",", ":")   # Compact JSON, bỏ khoảng trắng
        ).encode("utf-8"),

        # [MEMORY] Tổng buffer RAM của Producer tối đa 64MB
        buffer_memory=67_108_864,

        # Gom nhiều record thành một batch (tối đa 64KB) để tăng throughput
        batch_size=65_536,

        # Chờ tối đa 20ms để gom đủ batch trước khi gửi
        # Trade-off: tăng linger_ms → throughput cao hơn, latency cao hơn
        linger_ms=20,

        # [NETWORKING] Nén payload bằng gzip để giảm băng thông Tailscale
        # Compression ratio cho JSON số nguyên: ~4:1 → 400KB → ~100KB/chunk
        compression_type="gzip",

        # acks="all": Kafka Leader chờ tất cả in-sync replicas xác nhận
        # Với 1 broker, ISR = 1 → tương đương acks=1 nhưng explicit hơn
        acks="all",

        # Tự retry 3 lần khi gặp lỗi transient (network hiccup Tailscale)
        retries=3,
        retry_backoff_ms=500,

        # [NETWORKING] Timeout các network operations
        request_timeout_ms=30_000,      # Chờ tối đa 30s cho mỗi request
        connections_max_idle_ms=60_000, # Đóng connection idle > 60s
    )


# ─── Hàm gửi chunks vào Kafka ─────────────────────────────────────────────────

def send_chunks(producer: KafkaProducer, data: list, topic: str) -> None:
    """
    Chia mảng data thành các chunk và gửi tuần tự vào Kafka.

    [MEMORY] Kỹ thuật Chunking — tại sao cần thiết:
      - Serialize 1M int nguyên thành JSON: ~8MB uncompressed.
      - Kafka default max message size: 1MB → KHÔNG gửi được 8MB.
      - Chunking: mỗi lần chỉ serialize 50K int → ~400KB → sau gzip ~100KB.
      - Mỗi chunk fit trong memory của một JVM Kafka broker record.
      - Consumer (OpenFaaS function) cũng chỉ cần allocate 50K int (~1.4MB)
        working memory thay vì 1M int (~28MB) → tránh OOM trong pod.

    Cấu trúc payload JSON mỗi chunk:
      {
        "chunk_id":     <int>,   # Để consumer tracking và logging
        "total_chunks": <int>,   # Consumer biết khi nào xử lý xong tất cả
        "size":         <int>,   # Kích thước thực (chunk cuối có thể < CHUNK_SIZE)
        "timestamp_ms": <int>,   # Để đo end-to-end latency Kafka → Function
        "data":         [...]    # Mảng số nguyên cần sắp xếp
      }
    """
    total = len(data)
    # Ceiling division: đảm bảo chunk cuối không bị bỏ sót
    total_chunks = (total + CHUNK_SIZE - 1) // CHUNK_SIZE

    logger.info(
        f"Bắt đầu gửi {total_chunks} chunks × {CHUNK_SIZE:,} phần tử "
        f"→ topic '{topic}'"
    )

    t_start = time.perf_counter()
    sent_count = 0

    for chunk_id in range(total_chunks):
        # Tính vị trí slice cho chunk hiện tại
        idx_start = chunk_id * CHUNK_SIZE
        idx_end   = min(idx_start + CHUNK_SIZE, total)

        # [MEMORY] list slice tạo bản copy nhỏ (~50K × 28B = ~1.4MB)
        # Không duplicate toàn bộ mảng gốc 1M phần tử.
        chunk_data = data[idx_start:idx_end]

        payload = {
            "chunk_id":     chunk_id,
            "total_chunks": total_chunks,
            "size":         len(chunk_data),
            "timestamp_ms": int(time.time() * 1000),
            "data":         chunk_data,
        }

        try:
            # send() là non-blocking (đẩy vào internal buffer)
            # future.get() là blocking — đảm bảo chunk được broker nhận trước
            # khi chuyển sang chunk tiếp theo (đảm bảo thứ tự và phát hiện lỗi sớm)
            future   = producer.send(topic, value=payload)
            metadata = future.get(timeout=30)  # Block tối đa 30s

            sent_count += 1
            logger.info(
                f"  [{chunk_id + 1:>3}/{total_chunks}] ✓ "
                f"Partition={metadata.partition} | "
                f"Offset={metadata.offset:<8} | "
                f"Elements={len(chunk_data):,}"
            )

        except KafkaTimeoutError:
            logger.error(
                f"  [{chunk_id + 1}/{total_chunks}] ✗ TIMEOUT — "
                f"Kafka broker không phản hồi trong 30s. "
                f"Kiểm tra kết nối mạng LAN và broker status."
            )
            raise
        except KafkaError as exc:
            logger.error(f"  [{chunk_id + 1}/{total_chunks}] ✗ Kafka error: {exc}")
            raise

    elapsed = time.perf_counter() - t_start
    throughput = total / elapsed if elapsed > 0 else 0

    logger.info(
        f"\n{'═' * 60}\n"
        f"  Hoàn thành gửi {sent_count}/{total_chunks} chunks\n"
        f"  Tổng phần tử   : {total:,}\n"
        f"  Tổng thời gian : {elapsed:.2f}s\n"
        f"  Throughput     : {throughput:,.0f} phần tử/giây\n"
        f"{'═' * 60}"
    )


# ─── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Kafka Chunker Producer — NT533 Distributed Systems Lab",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--broker",
        default=DEFAULT_BROKER,
        help="Địa chỉ Kafka bootstrap server (host:port)",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC_NAME,
        help="Tên Kafka topic đích",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=TOTAL_ELEMENTS,
        help="Tổng số phần tử cần sinh",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=CHUNK_SIZE,
        dest="chunk_size",
        help="Số phần tử mỗi chunk",
    )
    args = parser.parse_args()

    logger.info(f"{'═' * 60}")
    logger.info(f"  NT533 — Kafka Chunker Producer")
    logger.info(f"  Broker     : {args.broker}")
    logger.info(f"  Topic      : {args.topic}")
    logger.info(f"  Total      : {args.total:,} phần tử")
    logger.info(f"  Chunk size : {args.chunk_size:,} phần tử")
    logger.info(f"{'═' * 60}")

    producer = create_producer(args.broker)

    try:
        # Bước 1: Sinh dữ liệu ngẫu nhiên vào RAM
        data = generate_data(args.total)

        # Bước 2: Chunking và gửi vào Kafka
        send_chunks(producer, data, args.topic)

    finally:
        # Đảm bảo flush toàn bộ internal buffer trước khi đóng connection
        logger.info("Đang flush buffer Producer...")
        producer.flush(timeout=60)
        producer.close(timeout=10)
        logger.info("Producer đã đóng kết nối.")


if __name__ == "__main__":
    main()
