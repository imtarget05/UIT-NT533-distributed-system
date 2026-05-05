#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  connector.py — Kafka → OpenFaaS Event Bridge (Kafka Connector)            ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Bước 2 của MapReduce Pipeline: SHUFFLE (Event-Driven)

Kiến trúc:
  [Kafka Topic "UIT"]
       ↓ (Connector poll mỗi 200ms)
  [Read message: JSON {chunk_id, data: [...]}]
       ↓
  [HTTP POST → OpenFaaS Gateway /function/parallel-sort]
       ↓
  [parallel-sort pod nhận & xử lý]
       ↓
  [If HTTP 200: Commit offset]
  [If HTTP ≠ 200: KHÔNG commit → Retry auto]
       ↓
  [Kafka Topic "UIT-OUTPUT" nhận sorted chunk]

At-Least-Once Delivery:
  - Offset chỉ commit SAU khi function trả HTTP 200
  - Nếu pod crash trước HTTP 200 → Kafka giữ offset → Connector retry

Cách chạy:
  # Trực tiếp (yêu cầu env vars):
  python3 connector.py

  # Hoặc với args:
  python3 connector.py --broker kafka.kafka.svc.cluster.local:9092 \
    --topic UIT --gateway http://gateway.openfaas.svc.cluster.local:8080

  # Trong K3s (từ ConfigMap):
  kubectl create configmap kafka-connector-script \
    --from-file=connector.py \
    -n openfaas
"""

import json
import os
import sys
import time
import logging
import argparse
import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from base64 import b64encode

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Hằng số ──────────────────────────────────────────────────────────────────
DEFAULT_BROKER = os.getenv(
    "KAFKA_BROKER", "kafka.kafka.svc.cluster.local:9092"
)
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC_IN", "UIT")
DEFAULT_GATEWAY = os.getenv(
    "GATEWAY_URL", "http://gateway.openfaas.svc.cluster.local:8080"
)
DEFAULT_FUNCTION = os.getenv("FUNCTION_NAME", "parallel-sort")
DEFAULT_GROUP = os.getenv(
    "GROUP_ID", f"connector-s3-{int(time.time())}"
)  # Unique per restart

# OpenFaaS Basic Auth
OPENFAAS_USER = os.getenv("GW_USER", "admin")
OPENFAAS_PASS = os.getenv("GW_PASS", "")

# ─── Hàm tiện ích ─────────────────────────────────────────────────────────────


def get_basic_auth_header(username, password):
    """Tạo HTTP Basic Auth header"""
    credentials = f"{username}:{password}".encode()
    b64_credentials = b64encode(credentials).decode()
    return {"Authorization": f"Basic {b64_credentials}"}


def invoke_function(gateway_url, function_name, payload, auth_headers):
    """
    Gọi OpenFaaS function qua HTTP POST

    Returns:
        (success: bool, response_code: int, response_body: str)
    """
    url = f"{gateway_url}/function/{function_name}"

    try:
        response = requests.post(
            url,
            json=payload,
            headers={**auth_headers, "Content-Type": "application/json"},
            timeout=60,
        )
        return (response.status_code == 200, response.status_code, response.text)

    except requests.exceptions.Timeout:
        logger.error(f"✗ Function timeout: {function_name}")
        return (False, 504, "Timeout")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"✗ Connection error: {e}")
        return (False, 503, "Connection error")
    except Exception as e:
        logger.error(f"✗ Unexpected error: {e}")
        return (False, 500, str(e))


def main():
    parser = argparse.ArgumentParser(
        description="Kafka Connector: Event bridge từ Kafka → OpenFaaS"
    )
    parser.add_argument(
        "--broker", default=DEFAULT_BROKER, help="Kafka broker address"
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic")
    parser.add_argument(
        "--gateway", default=DEFAULT_GATEWAY, help="OpenFaaS gateway URL"
    )
    parser.add_argument(
        "--function", default=DEFAULT_FUNCTION, help="OpenFaaS function name"
    )
    parser.add_argument(
        "--group", default=DEFAULT_GROUP, help="Kafka consumer group"
    )
    parser.add_argument(
        "--poll-timeout",
        type=int,
        default=200,
        help="Poll timeout (ms, default 200)",
    )

    args = parser.parse_args()

    logger.info("🔀 Kafka Connector — Event Bridge (Kafka → OpenFaaS)")
    logger.info(f"  Broker: {args.broker}")
    logger.info(f"  Topic: {args.topic}")
    logger.info(f"  Group: {args.group}")
    logger.info(f"  Gateway: {args.gateway}")
    logger.info(f"  Function: {args.function}")
    logger.info("")

    # ─── Basic Auth header ────────────────────────────────────────────────────
    auth_headers = {}
    if OPENFAAS_USER and OPENFAAS_PASS:
        auth_headers = get_basic_auth_header(OPENFAAS_USER, OPENFAAS_PASS)
        logger.info(f"✓ Using Basic Auth: user={OPENFAAS_USER}")
    else:
        logger.warning("⚠ No OpenFaaS credentials provided (may be unauthenticated)")

    # ─── Khởi tạo Kafka Consumer ──────────────────────────────────────────────
    try:
        consumer = KafkaConsumer(
            args.topic,
            bootstrap_servers=args.broker,
            group_id=args.group,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit khi HTTP 200
            value_deserializer=lambda m: m,  # Raw bytes (sẽ parse JSON sau)
            session_timeout_ms=30000,
            request_timeout_ms=60000,
        )
        logger.info(f"✓ Connected to Kafka: {args.broker}")
        logger.info(
            f"✓ Subscribed to topic: {args.topic} (consumer group: {args.group})"
        )
    except KafkaError as e:
        logger.error(f"✗ Failed to connect Kafka: {e}")
        sys.exit(1)

    logger.info("")
    logger.info("⏳ Connector started. Polling for messages...\n")

    # ─── Main loop: Poll → Invoke → Commit ────────────────────────────────────
    message_count = 0
    success_count = 0
    error_count = 0

    try:
        for message in consumer:
            try:
                # Parse message
                try:
                    payload = json.loads(message.value.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    logger.warning(f"⚠ Failed to parse message. Skipping.")
                    continue

                chunk_id = payload.get("chunk_id", "?")
                data_len = len(payload.get("data", []))
                message_count += 1

                logger.info(
                    f"[{message_count:3d}] Invoking {args.function} "
                    f"(chunk_id={chunk_id}, size={data_len:,})"
                )

                # Invoke function
                success, status_code, response = invoke_function(
                    args.gateway, args.function, payload, auth_headers
                )

                if success:
                    logger.info(
                        f"      ✓ HTTP {status_code}: Function executed successfully"
                    )
                    success_count += 1

                    # ✓ Commit offset (At-Least-Once: commit SAU successful invocation)
                    try:
                        consumer.commit()
                        logger.info(f"      ✓ Offset committed (partition={message.partition}, offset={message.offset})")
                    except Exception as e:
                        logger.warning(f"      ⚠ Failed to commit offset: {e}")

                else:
                    logger.error(
                        f"      ✗ HTTP {status_code}: Function invocation failed"
                    )
                    error_count += 1
                    logger.info(
                        f"      ℹ Not committing offset → Message will be retried"
                    )

            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.error(f"✗ Unexpected error processing message: {e}")
                error_count += 1
                continue

    except KeyboardInterrupt:
        logger.info("\n🛑 Connector stopped by user")
    finally:
        consumer.close()
        logger.info("")
        logger.info("═" * 80)
        logger.info(f"📊 Connector Statistics:")
        logger.info(f"  Total messages processed: {message_count}")
        logger.info(f"  Successful invocations:  {success_count}")
        logger.info(f"  Failed invocations:      {error_count}")
        logger.info("═" * 80)


if __name__ == "__main__":
    main()
