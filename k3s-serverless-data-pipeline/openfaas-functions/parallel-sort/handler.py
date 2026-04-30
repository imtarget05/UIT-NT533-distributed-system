#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  handler.py — Parallel Quicksort Function (OpenFaaS)                       ║
║  Đồ án NT533: K3s Serverless Data Pipeline                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Kiến trúc xử lý (Fork-Join Pattern):

  Kafka Topic 'UIT'
       │
       ▼
  kafka-connector (OpenFaaS)
       │  HTTP POST /function/parallel-sort
       │  Body: JSON payload từ Kafka message
       ▼
  ┌─────────────────────────────────────────────────────────────┐
  │  OpenFaaS Function: parallel-sort                           │
  │                                                             │
  │  handle(event, context)                                     │
  │       │                                                     │
  │       ├── Parse JSON → list[int] (50.000 phần tử)          │
  │       │                                                     │
  │       └── parallel_quicksort(data)                         │
  │              │                                              │
  │              ├── [SPLIT]  Prefix-Sum Scatter partition      │
  │              │            → L (less), E (equal), R (greater)│
  │              │                                              │
  │              │     ┌──── Process L: _pq_worker(L) ──────┐   │
  │              │     └──── Process R: _pq_worker(R) ──────┘   │
  │              │           (max_workers=2, max_depth=3)       │
  │              │                                              │
  │              └── [JOIN]  ls + e + rs (concatenation)       │
  │                          → sorted list[int]                 │
  │                                                             │
  │  Trả về: JSON {"chunk_id":..., "data": [...sorted...]}     │
  └─────────────────────────────────────────────────────────────┘

Tính chất:
  - STATELESS: không lưu bất kỳ trạng thái nào giữa các invocation.
  - IDEMPOTENT: cùng input luôn cho cùng output.
  - SAFE TO SCALE: nhiều replica chạy song song không gây conflict.
"""

import json
import os
import sys
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

# ─── Cấu hình logging ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Tham số từ Environment Variables ────────────────────────────────────────

# [MULTI-PROCESSING] Ngưỡng tối thiểu để kích hoạt song song.
# Nếu n < PARALLEL_THRESHOLD: fallback sang sequential để tránh IPC overhead.
# Cross-over point thực đo với 'spawn' start method: ~10.000 phần tử
# (spawn overhead ~2–5ms > ~0.5ms sort time khi n < 10K).
PARALLEL_THRESHOLD: int = int(os.environ.get("PARALLEL_THRESHOLD", "10000"))

# [MULTI-PROCESSING] Độ sâu đệ quy song song tối đa.
# Mỗi level tăng 1 sẽ nhân đôi số process: tổng tối đa = 2^MAX_DEPTH processes.
# Mặc định 3 → tối đa 8 processes song song → phù hợp Worker node 4–8 core.
# CẢNH BÁO: MAX_DEPTH > 4 gây process explosion (2^5 = 32 processes → OOM pod).
MAX_DEPTH: int = int(os.environ.get("MAX_DEPTH", "3"))

# [MEMORY] Tăng recursion limit cho sequential_quicksort (worst-case O(N) depth).
# Với threshold=10000, sequential chỉ xử lý sub-array <= 10000 phần tử.
# Avg depth = O(log 10000) ≈ 14 — an toàn. Worst case (đã sorted) = 10000.
# Đặt 20000 để có headroom; mặc định Python = 1000 (quá thấp).
sys.setrecursionlimit(max(20000, sys.getrecursionlimit()))

# ─── Cấu hình Multiprocessing Start Method ───────────────────────────────────
# [MULTI-PROCESSING] 'spawn' vs 'fork':
#
#  'fork'  : Copy toàn bộ memory space của parent. Nhanh (~1ms) nhưng nguy
#            hiểm khi parent multi-threaded (gunicorn watchdog của OpenFaaS):
#            lock của thread bị copy ở trạng thái locked → deadlock.
#
#  'spawn' : Tạo Python interpreter sạch. Chậm hơn (~100–300ms/worker) nhưng
#            an toàn tuyệt đối. ĐÚNG lựa chọn cho môi trường gunicorn/container.
#
# Lưu ý quan trọng với thuật toán đệ quy song song bên dưới:
#   Mỗi level đệ quy tạo 1 ProcessPoolExecutor(max_workers=2) mới.
#   Với MAX_DEPTH=3: tổng tối đa 2^3 = 8 worker processes đồng thời.
#   Mỗi 'spawn' worker mất ~150ms khởi động → pipeline latency tổng ~450ms.
#   Trade-off: spawn an toàn > overhead thời gian khởi động.
try:
    multiprocessing.set_start_method("spawn", force=False)
except RuntimeError:
    pass  # Start method đã được set trước đó (xảy ra khi pytest import module)


# ══════════════════════════════════════════════════════════════════════════════
# THUẬT TOÁN PARALLEL QUICKSORT — Prefix-Sum Scatter Partition
# Nguồn: Đồ án NT533 — thuật toán từ quicksort.py của nhóm
# ══════════════════════════════════════════════════════════════════════════════

def prefix_sum(x: list) -> list:
    """
    Tính exclusive prefix sum: y[i] = sum(x[0:i]).

    Đây là primitive cốt lõi của các thuật toán song song (scan/scatter).
    Được dùng trong parallel_partition để tính vị trí đích của mỗi phần tử
    mà không cần critical section hay mutex — không có data race.

    Ví dụ: x = [1, 0, 1, 1, 0] → y = [0, 1, 1, 2, 3]
    Nghĩa: phần tử thứ i sẽ được đặt vào vị trí y[i] trong output array.
    """
    s = 0
    y = [0] * len(x)
    for i, v in enumerate(x):
        y[i] = s
        s += v
    return y


def parallel_partition(a: list, pivot: int) -> tuple:
    """
    Phân hoạch mảng thành 3 phần (L, E, R) theo kỹ thuật Prefix-Sum Scatter.

    Thuật toán (3 bước):
      1. Flag arrays: flag_l[i]=1 nếu a[i]<pivot, flag_e[i]=1 nếu ==, flag_r[i] nếu >
      2. Prefix sum: pos_l = prefix_sum(flag_l) → vị trí đích của từng phần tử
      3. Scatter   : b[pos_l[i]] = a[i] nếu flag_l[i], tương tự cho E và R

    [MEMORY] Out-of-place: tạo output array b[n] → O(N) extra space (~1.4MB cho 50K int).
    Trade-off với in-place partition: đơn giản hơn để parallelize vì không có
    data hazard — mỗi phần tử ghi vào vị trí riêng biệt, không overlap.

    Tính song song thực sự đạt được ở tầng TRÊN qua ProcessPoolExecutor:
    các cuộc gọi parallel_partition khác nhau chạy trên CPU core khác nhau.
    """
    n = len(a)
    if n == 0:
        return [], [], []

    flag_l = [1 if v < pivot else 0 for v in a]
    flag_e = [1 if v == pivot else 0 for v in a]
    flag_r = [1 if v > pivot else 0 for v in a]

    pos_l = prefix_sum(flag_l)
    pos_e = prefix_sum(flag_e)
    pos_r = prefix_sum(flag_r)

    # Kích thước mỗi partition: giá trị cuối prefix_sum + flag cuối
    size_l = pos_l[-1] + flag_l[-1]
    size_e = pos_e[-1] + flag_e[-1]

    b = [0] * n

    # Scatter: đặt mỗi phần tử vào đúng vị trí đích trong b
    for i, v in enumerate(a):
        if flag_l[i]:
            b[pos_l[i]] = v
        elif flag_e[i]:
            b[pos_e[i] + size_l] = v
        else:
            b[pos_r[i] + size_l + size_e] = v

    return b[:size_l], b[size_l:size_l + size_e], b[size_l + size_e:n]


def sequential_quicksort(a: list) -> list:
    """
    Quicksort đệ quy đơn luồng (out-of-place, functional style).

    Là base case của parallel_quicksort khi:
      - n < PARALLEL_THRESHOLD : IPC overhead > lợi ích song song
      - max_depth <= 0         : đã đạt giới hạn độ sâu song song

    Pivot = phần tử giữa (index n//2):
      - Tránh worst-case O(N²) với mảng đã sắp xếp (pivot luôn = median nếu sorted).
      - Nhưng có thể gây O(N²) nếu input có nhiều phần tử trùng nhau và pivot
        luôn = phần tử trùng → tất cả vào một partition.
      - Với dữ liệu ngẫu nhiên từ chunker_producer.py: trung bình O(N log N).

    [MEMORY] Out-of-place: tạo sub-arrays L, E, R mới ở mỗi level đệ quy.
    Tổng space = O(N log N) average — chấp nhận được cho n <= PARALLEL_THRESHOLD.
    """
    if len(a) <= 1:
        return a
    pivot = a[len(a) // 2]
    l = [x for x in a if x < pivot]
    e = [x for x in a if x == pivot]
    r = [x for x in a if x > pivot]
    return sequential_quicksort(l) + e + sequential_quicksort(r)


# ══════════════════════════════════════════════════════════════════════════════
# WORKER FUNCTION — Top-level bắt buộc để pickle được qua IPC pipe
# ══════════════════════════════════════════════════════════════════════════════

def _pq_worker(a: list, threshold: int, max_depth: int) -> list:
    """
    [MULTI-PROCESSING] Top-level worker — gọi lại parallel_quicksort đệ quy.

    BẮT BUỘC là top-level function:
      multiprocessing dùng pickle để serialize function object trước khi gửi
      sang worker process qua IPC pipe. Lambda, nested function, method của class
      KHÔNG pickle được → RuntimeError: Can't pickle local object.

    Mô hình đệ quy song song (Recursive Parallelism):
      Depth 0: 1 pool × 2 workers → sort L và R song song
      Depth 1: 2 pools × 2 workers → 4 processes đồng thời
      Depth 2: 4 pools × 2 workers → 8 processes đồng thời
      Depth d: 2^d processes tổng cộng

    Tổng processes tối đa = sum(2^i, i=0..MAX_DEPTH) = 2^(MAX_DEPTH+1) - 1.
    Với MAX_DEPTH=3 → tối đa 15 processes — phù hợp Worker node 4–8 core.
    """
    return parallel_quicksort(a, threshold=threshold, max_depth=max_depth)


# ══════════════════════════════════════════════════════════════════════════════
# PARALLEL QUICKSORT — Divide-and-Conquer với ProcessPoolExecutor đệ quy
# ══════════════════════════════════════════════════════════════════════════════

def parallel_quicksort(a: list, threshold: int = 10_000, max_depth: int = 3) -> list:
    """
    Parallel Quicksort theo mô hình Divide-and-Conquer + Fork-Join đệ quy.

    Thuật toán:
      1. Partition bằng prefix-sum scatter → L (< pivot), E (== pivot), R (> pivot).
      2. Fork: submit _pq_worker(L) và _pq_worker(R) vào ProcessPoolExecutor.
         → Hai sub-array được sort song song trên 2 CPU core riêng biệt (bypass GIL).
      3. Join: thu kết quả → ls + e + rs.

    Điều kiện dừng (base cases):
      - n <= 1           : mảng đã sort, trả về ngay
      - n < threshold    : IPC overhead > lợi ích → sequential fallback
      - max_depth <= 0   : đạt giới hạn depth → sequential fallback

    Độ phức tạp (average case, dữ liệu ngẫu nhiên):
      - Parallel phase : O((N / 2^d) × log(N / 2^d)) mỗi process
      - Tổng wall time : O(N/P × log N) với P = 2^max_depth processes
      - Merge          : O(N) concatenation (không cần heapq vì L/E/R đã phân hoạch)

    [MULTI-PROCESSING] max_workers=2 trong mỗi ProcessPoolExecutor:
      QUAN TRỌNG — KHÔNG để mặc định (os.cpu_count()) vì mỗi worker cũng tạo
      pool riêng với max_workers=os.cpu_count() → số process tăng theo hàm mũ:
        os.cpu_count()^max_depth → 8^3 = 512 processes → resource exhaustion.
      Explicit max_workers=2 đảm bảo tổng processes = 2^(max_depth+1) - 1 (bounded).

    [MEMORY] Pickle overhead qua IPC pipe:
      - Depth 0: serialize L (~N/2 int) + R (~N/2 int) ≈ N × 8 bytes (pickle)
      - Depth 1: mỗi worker serialize N/4 int × 2 = N/2 int tổng
      - Tổng pickle overhead tất cả levels ≈ O(N × MAX_DEPTH) bytes ≈ 4MB với N=50K, d=3

    [NETWORKING] Không có network call — xử lý thuần CPU trong memory của pod.
    Kafka message đã được nhận bởi handle() trước khi gọi hàm này.
    """
    n = len(a)
    if n <= 1:
        return a
    if n < threshold or max_depth <= 0:
        return sequential_quicksort(a)

    pivot = a[n // 2]
    l, e, r = parallel_partition(a, pivot)

    # [MULTI-PROCESSING] Spawn đúng 2 workers: một cho L, một cho R.
    # max_workers=2 là giới hạn cứng chống process explosion:
    #   Không đặt mặc định → os.cpu_count() workers → 8^depth processes → OOM.
    with ProcessPoolExecutor(max_workers=2) as executor:
        future_l = executor.submit(_pq_worker, l, threshold, max_depth - 1)
        future_r = executor.submit(_pq_worker, r, threshold, max_depth - 1)
        try:
            ls = future_l.result()
            rs = future_r.result()
        except Exception as exc:
            logger.error(f"Worker process thất bại ở depth={max_depth}: {exc}")
            raise RuntimeError(f"Parallel sort worker failed: {exc}") from exc

    return ls + e + rs


# ══════════════════════════════════════════════════════════════════════════════
# OPENFAAS HANDLER — Entry Point
# ══════════════════════════════════════════════════════════════════════════════

def handle(event, context):
    """
    OpenFaaS HTTP Handler — Entry point được gọi bởi python3-http watchdog.

    Kafka Connector flow:
      1. kafka-connector poll message từ Kafka topic 'UIT'.
      2. Gọi HTTP POST tới http://gateway/function/parallel-sort.
      3. Request body = raw bytes của Kafka message value.
      4. Hàm này nhận event.body, parse JSON, sort, trả về JSON.

    Hàm này là STATELESS:
      - Không có biến global bị mutate.
      - Không có filesystem I/O.
      - Không có network calls.
      - An toàn khi nhiều replica chạy đồng thời trên các pod khác nhau.
    """
    import time

    t_start = time.perf_counter()

    # ── 1. Parse request body ──────────────────────────────────────────────────
    try:
        body = event.body
        if isinstance(body, (bytes, bytearray)):
            body = body.decode("utf-8")

        payload = json.loads(body)

    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning(f"Lỗi parse JSON: {exc}")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"JSON không hợp lệ: {exc}"}),
            "headers": {"Content-Type": "application/json"},
        }

    # ── 2. Trích xuất data array ──────────────────────────────────────────────
    # Hỗ trợ 2 định dạng payload:
    #   Dạng 1 (raw array): [1, 5, 3, ...]
    #   Dạng 2 (object):    {"chunk_id": 0, "data": [1, 5, 3, ...], ...}
    try:
        if isinstance(payload, list):
            data = payload
            chunk_id = "unknown"
            total_chunks = 1

        elif isinstance(payload, dict) and "data" in payload:
            data = payload["data"]
            chunk_id    = payload.get("chunk_id", "unknown")
            total_chunks = payload.get("total_chunks", 1)

        else:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Payload phải là JSON array hoặc object có trường 'data'"
                }),
                "headers": {"Content-Type": "application/json"},
            }

        if not isinstance(data, list):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Trường 'data' phải là JSON array"}),
                "headers": {"Content-Type": "application/json"},
            }

    except (TypeError, KeyError) as exc:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Lỗi truy cập payload: {exc}"}),
            "headers": {"Content-Type": "application/json"},
        }

    # ── 3. Thực hiện Parallel Quicksort ──────────────────────────────────────
    n = len(data)
    logger.info(
        f"Bắt đầu sort | chunk_id={chunk_id} | "
        f"size={n:,} | max_depth={MAX_DEPTH} | threshold={PARALLEL_THRESHOLD}"
    )

    try:
        t_sort = time.perf_counter()
        sorted_data = parallel_quicksort(
            data,
            threshold=PARALLEL_THRESHOLD,
            max_depth=MAX_DEPTH,
        )
        sort_elapsed_ms = (time.perf_counter() - t_sort) * 1000

    except RuntimeError as exc:
        logger.error(f"Sort thất bại: {exc}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Sort failed: {exc}"}),
            "headers": {"Content-Type": "application/json"},
        }

    # ── 4. Trả về kết quả ────────────────────────────────────────────────────
    total_elapsed_ms = (time.perf_counter() - t_start) * 1000

    logger.info(
        f"Hoàn thành | chunk_id={chunk_id} | "
        f"sort={sort_elapsed_ms:.1f}ms | total={total_elapsed_ms:.1f}ms"
    )

    # Số worker processes tối đa đã có thể được dùng:
    # Với recursive parallelism depth d, tổng tối đa = 2^d processes.
    workers_used = (2 ** MAX_DEPTH) if n >= PARALLEL_THRESHOLD else 1

    result = {
        "chunk_id":      chunk_id,
        "total_chunks":  total_chunks,
        "size":          n,
        "sorted":        True,
        "sort_ms":       round(sort_elapsed_ms, 2),
        "total_ms":      round(total_elapsed_ms, 2),
        "workers_used":  workers_used,
        "parallel_mode": n >= PARALLEL_THRESHOLD,
        "max_depth":     MAX_DEPTH,
        "threshold":     PARALLEL_THRESHOLD,
        "data":          sorted_data,
    }

    return {
        "statusCode": 200,
        # separators=(",", ":") → compact JSON, không có khoảng trắng thừa
        "body": json.dumps(result, separators=(",", ":")),
        "headers": {"Content-Type": "application/json"},
    }
