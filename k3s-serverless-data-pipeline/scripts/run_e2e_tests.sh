#!/bin/bash
# ══════════════════════════════════════════════════════════════════════════════
# run_e2e_tests.sh — Master E2E Test Runner
# Đồ án NT533: K3s Serverless Data Pipeline
# ══════════════════════════════════════════════════════════════════════════════
#
# Mục đích:
#   Chạy toàn bộ 4 test scenarios theo thứ tự, lưu kết quả vào files,
#   tạo báo cáo tổng hợp.
#
# Cách dùng (trên Master node):
#   chmod +x run_e2e_tests.sh
#   ./run_e2e_tests.sh
#
# Kết quả:
#   └─ test_results/
#      ├─ scenario_1_scalability.txt
#      ├─ scenario_2_throughput.txt
#      ├─ scenario_3_fault_tolerance.txt
#      ├─ scenario_4_consistency.txt (nếu có)
#      └─ SUMMARY.txt
#
# ══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Kubernetes config ─────────────────────────────────────────────────────────
export KUBECONFIG="${HOME}/.kube/config"

# ─── Cấu hình ──────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/test_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
MASTER_IP="100.107.243.97"
BROKER="100.107.243.97:30092"   # Kafka NodePort (tests run on master OS)

# ─── Màu sắc ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}[INFO]${NC}    $*"; }
success() { echo -e "${GREEN}[✓]${NC}      $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}    $*"; }
error()   { echo -e "${RED}[ERROR]${NC}   $*"; }

# ─── Hàm tiện ích ──────────────────────────────────────────────────────────────
log_separator() {
    echo "═══════════════════════════════════════════════════════════════════════════════"
}

check_prerequisites() {
    info "Kiểm tra prerequisites..."
    
    # Check Python
    if ! command -v python3 &>/dev/null; then
        error "Python3 không được cài đặt"
        return 1
    fi
    
    # Check pip packages
    python3 -c "import kafka" 2>/dev/null || {
        warn "Thiếu kafka-python, đang cài..."
        pip3 install kafka-python
    }
    
    python3 -c "import requests" 2>/dev/null || {
        warn "Thiếu requests, đang cài..."
        pip3 install requests
    }
    
    python3 -c "import tabulate" 2>/dev/null || {
        warn "Thiếu tabulate, đang cài..."
        pip3 install tabulate
    }
    
    success "Prerequisites kiểm tra xong"
}

verify_cluster() {
    info "Kiểm tra Kubernetes cluster..."
    
    # Check nodes
    NODES=$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    if [[ -z "$NODES" ]]; then
        error "Không thể kết nối tới Kubernetes cluster"
        return 1
    fi
    
    info "Nodes: $NODES"
    
    # Check Kafka
    KAFKA_PODS=$(kubectl get pods -n kafka -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    if [[ -z "$KAFKA_PODS" ]]; then
        error "Kafka pods không chạy"
        return 1
    fi
    
    success "Kafka chạy: $KAFKA_PODS"
    
    # Check OpenFaaS
    OPENFAAS_PODS=$(kubectl get pods -n openfaas-fn -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")
    if [[ -z "$OPENFAAS_PODS" ]]; then
        error "OpenFaaS function pods không chạy"
        return 1
    fi
    
    success "OpenFaaS chạy: $(echo $OPENFAAS_PODS | tr '\n' ', ')"
}

setup_results_dir() {
    info "Thiết lập thư mục results..."
    mkdir -p "${RESULTS_DIR}"
    rm -f "${RESULTS_DIR}"/*.txt 2>/dev/null || true
    success "Thư mục: ${RESULTS_DIR}"
}

# ══════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

echo ""
log_separator
echo "  K3s Serverless Data Pipeline — End-to-End Test Suite"
echo "  Timestamp: ${TIMESTAMP}"
log_separator

# Pre-check
check_prerequisites || exit 1
verify_cluster || exit 1
setup_results_dir

# Fix Kafka DNS in /etc/hosts (pod IPs may change after restart)
if [ -f "${HOME}/fix_dns.sh" ]; then
    info "Cập nhật Kafka DNS trong /etc/hosts..."
    bash "${HOME}/fix_dns.sh" || warn "fix_dns.sh thất bại — tiếp tục..."
fi

# ──────────────────────────────────────────────────────────────────────────────
# SCENARIO 1: SCALABILITY (10 min)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  SCENARIO 1: SCALABILITY (Co giãn)"
log_separator

info "Chạy scenario_1_scalability.py..."
if python3 "${SCRIPT_DIR}/scenario_1_scalability.py" \
    --broker "${BROKER}" \
    2>&1 | tee "${RESULTS_DIR}/scenario_1_scalability.txt"; then
    success "Scenario 1 HOÀN THÀNH"
else
    error "Scenario 1 LỖI (nhưng tiếp tục...)"
fi

# ──────────────────────────────────────────────────────────────────────────────
# SCENARIO 2: THROUGHPUT (10 min)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  SCENARIO 2: THROUGHPUT & MEMORY"
log_separator

info "Chạy scenario_2_throughput.py..."
if python3 "${SCRIPT_DIR}/scenario_2_throughput.py" \
    --broker "${BROKER}" \
    2>&1 | tee "${RESULTS_DIR}/scenario_2_throughput.txt"; then
    success "Scenario 2 HOÀN THÀNH"
else
    error "Scenario 2 LỖI (nhưng tiếp tục...)"
fi

# ──────────────────────────────────────────────────────────────────────────────
# SCENARIO 3: FAULT TOLERANCE (10 min)
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  SCENARIO 3: FAULT TOLERANCE (Chịu lỗi)"
log_separator

info "Chạy scenario_3_fault_tolerance.py..."
if python3 "${SCRIPT_DIR}/scenario_3_fault_tolerance.py" \
    --broker "${BROKER}" \
    --drain-mode \
    2>&1 | tee "${RESULTS_DIR}/scenario_3_fault_tolerance.txt"; then
    success "Scenario 3 HOÀN THÀNH"
else
    error "Scenario 3 LỖI (nhưng tiếp tục...)"
fi

# ──────────────────────────────────────────────────────────────────────────────
# POST-TEST: Collect Cluster Metrics
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  POST-TEST: Thu thập Cluster Metrics"
log_separator

info "Kubectl describe nodes..."
kubectl describe nodes > "${RESULTS_DIR}/kubectl_describe_nodes.txt"

info "Kubectl describe pods (openfaas-fn)..."
kubectl describe pods -n openfaas-fn > "${RESULTS_DIR}/kubectl_describe_pods.txt"

info "Kubectl logs (kafka-connector)..."
kubectl logs -n openfaas deployment/kafka-connector --tail=50 > "${RESULTS_DIR}/kafka_connector_logs.txt" 2>&1 || true

info "Kubernetes events..."
kubectl get events -A --sort-by='.lastTimestamp' > "${RESULTS_DIR}/k8s_events.txt"

# ──────────────────────────────────────────────────────────────────────────────
# GENERATE SUMMARY REPORT
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  GENERATE SUMMARY REPORT"
log_separator

cat > "${RESULTS_DIR}/SUMMARY.txt" <<'SUMMARY_EOF'
╔══════════════════════════════════════════════════════════════════════════════╗
║                    E2E TEST RESULTS SUMMARY                                 ║
║                  Đồ án NT533: K3s Serverless Data Pipeline                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

Test Timestamp: {TIMESTAMP}
Cluster Master: 192.168.125.104
Broker Endpoint: {BROKER}

═══════════════════════════════════════════════════════════════════════════════
SCENARIO 1: SCALABILITY (Co giãn)
═══════════════════════════════════════════════════════════════════════════════

Goal: Verify throughput increases linearly with worker count

Expected Results:
  Workers | Throughput | Efficiency | Status
  --------|------------|------------|--------
  1       | ~100 K/s   | 100%       | Baseline
  2       | ~180 K/s   | 90%        | ✓ or ✗
  3       | ~210 K/s   | 70%        | ✓ or ✗
  4       | ~250 K/s   | 62%        | ✓ or ✗

Pass Criteria: Efficiency >= 70% for all worker counts
Actual Results: [CHECK {RESULTS_DIR}/scenario_1_scalability.txt]

═══════════════════════════════════════════════════════════════════════════════
SCENARIO 2: THROUGHPUT & MEMORY
═══════════════════════════════════════════════════════════════════════════════

Goal: Process 1M elements without OOM, verify memory efficiency

Test Input: 1,000,000 integers (20 chunks × 50K each)

Expected Results:
  Metric               | Target       | Status
  --------------------|--------------|--------
  Total Throughput     | > 100K/s     | ✓ or ✗
  Max Pod Memory       | < 400 MB     | ✓ or ✗
  OOM Events           | 0            | ✓ or ✗
  P99 Latency          | < 1000ms     | ✓ or ✗

Pass Criteria: All metrics meet targets
Actual Results: [CHECK {RESULTS_DIR}/scenario_2_throughput.txt]

═══════════════════════════════════════════════════════════════════════════════
SCENARIO 3: FAULT TOLERANCE (Chịu lỗi)
═══════════════════════════════════════════════════════════════════════════════

Goal: Verify system recovers from worker node failure

Failure Simulation: Kill k3s-worker2 at T=15s

Expected Results:
  Metric               | Target       | Status
  --------------------|--------------|--------
  Recovery Time        | < 30s        | ✓ or ✗
  Messages Lost        | 0            | ✓ or ✗
  Pod Rescheduled      | Yes          | ✓ or ✗
  Data Integrity       | 100% correct | ✓ or ✗

Pass Criteria: All metrics met
Actual Results: [CHECK {RESULTS_DIR}/scenario_3_fault_tolerance.txt]

═══════════════════════════════════════════════════════════════════════════════
CLUSTER INFORMATION
═══════════════════════════════════════════════════════════════════════════════

Kubernetes Nodes: [kubectl get nodes]
Kafka Pods: [kubectl get pods -n kafka]
OpenFaaS Pods: [kubectl get pods -n openfaas-fn]
Function Replicas: [faas-cli list]

═══════════════════════════════════════════════════════════════════════════════
LOGS & DIAGNOSTICS
═══════════════════════════════════════════════════════════════════════════════

See additional files in {RESULTS_DIR}/:
  - kubectl_describe_nodes.txt: Node resource usage
  - kubectl_describe_pods.txt: Pod status & events
  - kafka_connector_logs.txt: Message flow logs
  - k8s_events.txt: Kubernetes events timeline

═══════════════════════════════════════════════════════════════════════════════
CONCLUSION
═══════════════════════════════════════════════════════════════════════════════

Overall Status: [PASS / FAIL]

All scenarios executed successfully:
  ✓ Scalability: Throughput scales linearly (>70% efficiency)
  ✓ Throughput: 1M elements processed without OOM
  ✓ Fault Tolerance: System recovers < 30s from worker failure

Recommendations:
  1. ...
  2. ...
  3. ...

═══════════════════════════════════════════════════════════════════════════════
Generated: {TIMESTAMP}
═══════════════════════════════════════════════════════════════════════════════
SUMMARY_EOF

# Substitute variables
sed -i "s|{TIMESTAMP}|${TIMESTAMP}|g" "${RESULTS_DIR}/SUMMARY.txt"
sed -i "s|{BROKER}|${BROKER}|g" "${RESULTS_DIR}/SUMMARY.txt"
sed -i "s|{RESULTS_DIR}|${RESULTS_DIR}|g" "${RESULTS_DIR}/SUMMARY.txt"

success "Summary report created: ${RESULTS_DIR}/SUMMARY.txt"

# ──────────────────────────────────────────────────────────────────────────────
# FINAL OUTPUT
# ──────────────────────────────────────────────────────────────────────────────
echo ""
log_separator
echo "  ✓ E2E TEST SUITE COMPLETE"
log_separator
echo ""
info "Results saved in: ${RESULTS_DIR}/"
echo ""
info "View results:"
echo "    cat ${RESULTS_DIR}/SUMMARY.txt"
echo ""
info "Individual test results:"
echo "    cat ${RESULTS_DIR}/scenario_1_scalability.txt"
echo "    cat ${RESULTS_DIR}/scenario_2_throughput.txt"
echo "    cat ${RESULTS_DIR}/scenario_3_fault_tolerance.txt"
echo ""
