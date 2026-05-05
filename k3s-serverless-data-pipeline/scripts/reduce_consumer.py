#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  reduce_consumer.py — K-way Merge Reduce Consumer                          ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Bước 4 của MapReduce Pipeline: REDUCE

Nhiệm vụ:
  - Subscribe Kafka topic 'UIT-OUTPUT' (kết quả sorted chunks từ parallel-sort function)
  - Collect tất cả 20 sorted chunks
  - Perform K-way merge (heapq.merge) O(N log k) để thu được mảng 1M phần tử sắp xếp
  - Verify tính toàn vẹn (min=0, max=999.999, is_sorted=YES)

Đặc tính:
  - IDEMPOTENT: Có thể chạy nhiều lần, kết quả luôn như nhau
  - STREAMING: Không load toàn bộ vào RAM, merge on-the-fly
  - FAULT-TOLERANT: At-Least-Once delivery từ Kafka

Cách chạy:
  # Local test (cần Kafka running):
  python3 reduce_consumer.py --broker localhost:9092 --topic UIT-OUTPUT

  # Trong K3s cluster:
  python3 reduce_consumer.py --broker kafka.kafka.svc.cluster.local:9092 --topic UIT-OUTPUT

Output:
  Dòng cuối cùng in ra: RESULT: min=0, max=999999, count=1000000, is_sorted=True
"""

import json
import argparse
import logging
import sys
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import heapq

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Hằng số ──────────────────────────────────────────────────────────────────
DEFAULT_BROKER = "kafka.kafka.svc.cluster.local:9092"  # In-cluster DNS
DEFAULT_TOPIC = "UIT-OUTPUT"
DEFAULT_GROUP = "reduce-consumer-group"
CHUNK_SIZE = 50_000  # Mỗi chunk tối đa 50K phần tử
TOTAL_CHUNKS = 20

def main():
    parser = argparse.ArgumentParser(
        description="Reduce Consumer: K-way merge sorted chunks từ Kafka"
    )
    parser.add_argument(
        "--broker",
        default=DEFAULT_BROKER,
        help=f"Kafka broker address (default: {DEFAULT_BROKER})",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic để subscribe (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--group",
        default=DEFAULT_GROUP,
        help=f"Consumer group ID (default: {DEFAULT_GROUP})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout (giây) chờ hết các chunks (default: 120)",
    )
    args = parser.parse_args()

    logger.info(f"🔄 Reduce Consumer — K-way Merge")
    logger.info(f"  Broker: {args.broker}")
    logger.info(f"  Topic: {args.topic}")
    logger.info(f"  Group: {args.group}")
    logger.info(f"  Expected chunks: {TOTAL_CHUNKS}")
    logger.info("")

    # ─── Khởi tạo Kafka Consumer ──────────────────────────────────────────────
    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.broker,
            group_id=args.group,
            auto_offset_reset="earliest",  # Đọc từ đầu topic
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            session_timeout_ms=30000,
            request_timeout_ms=60000,
            max_poll_records=1,  # Lấy 1 message/lần để kiểm soát bộ nhớ
        )
        logger.info(f"✓ Connected to Kafka: {args.broker}")
    except KafkaError as e:
        logger.error(f"✗ Failed to connect Kafka: {e}")
        sys.exit(1)

    # ─── Collect chunks & K-way merge ──────────────────────────────────────────
    chunks = {}
    received_count = 0
    all_received = False

    logger.info("⏳ Waiting for sorted chunks from parallel-sort function...")
    logger.info("")

    try:
        for message in consumer:
            try:
                chunk = message.value
                chunk_id = chunk.get("chunk_id")
                data = chunk.get("data", [])

                if chunk_id is not None:
                    chunks[chunk_id] = data
                    received_count += 1
                    logger.info(
                        f"  [{received_count:2d}] Received chunk_id={chunk_id:2d} "
                        f"(size={len(data):,} elements) — "
                        f"Progress: {received_count}/{TOTAL_CHUNKS}"
                    )

                    # Kiểm tra xem đã nhận đủ chunks chưa
                    if received_count >= TOTAL_CHUNKS:
                        all_received = True
                        logger.info("")
                        logger.info(f"✓ All {TOTAL_CHUNKS} chunks received!")
                        break

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                logger.warning(f"⚠ Failed to parse message: {e}")
                continue

        if not all_received:
            logger.warning(
                f"⚠ Timeout: Only received {received_count}/{TOTAL_CHUNKS} chunks"
            )

    except KeyboardInterrupt:
        logger.info("\n🛑 Consumer interrupted by user")
    finally:
        consumer.close()

    # ─── K-way Merge ──────────────────────────────────────────────────────────
    if received_count == 0:
        logger.error("✗ No chunks received. Exiting.")
        sys.exit(1)

    logger.info("")
    logger.info(f"🔀 Starting K-way merge ({received_count} chunks)...")

    # Sort chunk_ids để merge theo thứ tự
    sorted_chunk_ids = sorted(chunks.keys())

    # heapq.merge: stream merge multiple sorted iterables, O(N log k)
    try:
        merged = heapq.merge(*[chunks[cid] for cid in sorted_chunk_ids])
        result = list(merged)  # Consume iterator → list
        logger.info(f"✓ Merge completed: {len(result):,} elements")
    except Exception as e:
        logger.error(f"✗ Merge failed: {e}")
        sys.exit(1)

    # ─── Verify tính toàn vẹn ──────────────────────────────────────────────────
    logger.info("")
    logger.info("🔍 Verifying result...")

    try:
        assert len(result) == 1_000_000, f"Count mismatch: {len(result)} != 1000000"
        assert result[0] == 0, f"Min mismatch: {result[0]} != 0"
        assert result[-1] == 999_999, f"Max mismatch: {result[-1]} != 999999"

        # Check monotonic increasing
        for i in range(len(result) - 1):
            assert (
                result[i] <= result[i + 1]
            ), f"Not sorted at index {i}: {result[i]} > {result[i + 1]}"

        is_sorted = True
        logger.info("✓ All checks passed!")

    except AssertionError as e:
        logger.error(f"✗ Verification failed: {e}")
        is_sorted = False

    # ─── Final Result ──────────────────────────────────────────────────────────
    logger.info("")
    logger.info("=" * 80)
    if is_sorted:
        logger.info(
            f"✓ SUCCESS: 1M elements sorted and verified!\n"
            f"  RESULT: min={result[0]}, max={result[-1]}, count={len(result):,}, is_sorted=True"
        )
    else:
        logger.error(
            f"✗ FAILURE: Sorted data verification failed!\n"
            f"  RESULT: min={result[0] if result else 'N/A'}, "
            f"max={result[-1] if result else 'N/A'}, count={len(result)}, is_sorted=False"
        )
    logger.info("=" * 80)

    sys.exit(0 if is_sorted else 1)


if __name__ == "__main__":
    main()
