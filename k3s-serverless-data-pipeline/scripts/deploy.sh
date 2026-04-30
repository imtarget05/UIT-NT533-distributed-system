#!/bin/bash
# ══════════════════════════════════════════════════════════════════════════════
# deploy.sh — Script triển khai toàn bộ pipeline trên Master node (k3s-master)
# Đồ án NT533: K3s Serverless Data Pipeline
# ══════════════════════════════════════════════════════════════════════════════
#
# CÁCH DÙNG (chạy trên k3s-master: 192.168.125.104):
#   chmod +x deploy.sh
#   ./deploy.sh
#
# THỨ TỰ TRIỂN KHAI:
#   1. Cài Bitnami Kafka (KRaft, 1 broker)
#   2. Tạo Kafka topic uit-lab3
#   3. Kiểm tra OpenFaaS đã cài chưa
#   4. Kéo OpenFaaS template python3-http-debian
#   5. Build & push Docker image target05/parallel-sort:latest
#   6. Deploy OpenFaaS function
#   7. Cài Kafka connector
#   8. Tạo ConfigMap cho producer
#   9. Chạy Job producer
#
# YÊU CẦU TRƯỚC KHI CHẠY:
#   - kubectl đã cấu hình kết nối K3s cluster
#   - helm đã cài (helm version)
#   - faas-cli đã cài (faas-cli version)
#   - docker đã cài và đã đăng nhập Docker Hub (docker login)
#   - File cấu trúc thư mục đầy đủ (xem README)
#
# ══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ─── Cấu hình ─────────────────────────────────────────────────────────────────
MASTER_IP="192.168.125.104"
OPENFAAS_GATEWAY="http://127.0.0.1:8080"   # port-forward 8080 đang chạy
OPENFAAS_PASSWORD=""                         # Được tự điền ở bước 3

# ─── Màu sắc cho output ────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
success() { echo -e "${GREEN}[✓]${NC}    $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

echo "══════════════════════════════════════════════════════════════"
echo "  NT533 — Deploy K3s Serverless Data Pipeline"
echo "  Master: ${MASTER_IP}"
echo "══════════════════════════════════════════════════════════════"

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 1: Cài Apache Kafka (Bitnami Helm Chart, KRaft mode)
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 1: Kiểm tra / Cài Kafka..."

if helm status kafka -n kafka &>/dev/null; then
    warn "Kafka đã được cài. Bỏ qua bước 1."
    warn "Nếu muốn cài lại với cấu hình mới:"
    warn "  helm uninstall kafka -n kafka && kubectl delete pvc --all -n kafka"
else
    info "Thêm Bitnami Helm repo..."
    helm repo add bitnami https://charts.bitnami.com/bitnami 2>/dev/null || true
    helm repo update

    info "Cài Kafka vào namespace 'kafka'..."
    helm install kafka bitnami/kafka \
        --namespace kafka \
        --create-namespace \
        --version 26.8.5 \
        -f kubernetes-manifests/kafka-values.yaml \
        --wait \
        --timeout 10m

    success "Kafka đã cài xong."
fi

# Kiểm tra pod Kafka running
info "Kiểm tra Kafka pod..."
kubectl wait pod -l app.kubernetes.io/name=kafka \
    -n kafka --for=condition=ready --timeout=300s
success "Kafka pod đang chạy."

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 2: Tạo Kafka topic uit-lab3 (nếu chưa có)
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 2: Tạo Kafka topic 'uit-lab3'..."

KAFKA_POD=$(kubectl get pod -n kafka -l app.kubernetes.io/name=kafka \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [[ -z "$KAFKA_POD" ]]; then
    error "Không tìm thấy Kafka pod. Kiểm tra lại bước 1."
fi

# Kiểm tra topic đã tồn tại chưa
TOPIC_EXISTS=$(kubectl exec -n kafka "$KAFKA_POD" -- \
    /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep -c "uit-lab3" || true)

if [[ "$TOPIC_EXISTS" -gt 0 ]]; then
    warn "Topic 'UIT' đã tồn tại. Bỏ qua."
else
    kubectl exec -n kafka "$KAFKA_POD" -- \
        /opt/bitnami/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:9092 \
        --create \
        --topic UIT \
        --partitions 3 \
        --replication-factor 1
    success "Topic 'UIT' đã được tạo (3 partitions, RF=1)."
fi

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 3: Kiểm tra OpenFaaS
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 3: Kiểm tra OpenFaaS..."

if ! kubectl get namespace openfaas &>/dev/null; then
    error "OpenFaaS chưa được cài. Cài trước bằng:\n  curl -sLS https://get.arkade.dev | sudo sh\n  arkade install openfaas"
fi

kubectl wait pod -l app=gateway \
    -n openfaas --for=condition=ready --timeout=120s
success "OpenFaaS gateway đang chạy."

# Lấy mật khẩu OpenFaaS admin
OPENFAAS_PASSWORD=$(kubectl get secret -n openfaas basic-auth \
    -o jsonpath="{.data.basic-auth-password}" | base64 --decode)

info "Đăng nhập OpenFaaS CLI..."
echo -n "$OPENFAAS_PASSWORD" | faas-cli login \
    --gateway "$OPENFAAS_GATEWAY" \
    --username admin \
    --password-stdin
success "Đăng nhập thành công."

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 4: Kéo OpenFaaS template python3-http-debian
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 4: Kéo template 'python3-http-debian'..."

cd openfaas-functions/

if [[ -d "template/python3-http-debian" ]]; then
    warn "Template 'python3-http-debian' đã tồn tại. Bỏ qua."
else
    faas-cli template store pull python3-http-debian
    success "Template đã kéo xong."
fi

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 5: Build & Push Docker image
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 5: Build Docker image 'target05/parallel-sort:latest'..."
info "  (yêu cầu docker login target05 trước)"

faas-cli build -f stack.yml
success "Build xong."

info "Push image lên Docker Hub..."
faas-cli push -f stack.yml
success "Push xong: target05/parallel-sort:latest"

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 6: Deploy OpenFaaS function
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 6: Deploy function 'parallel-sort'..."

faas-cli deploy -f stack.yml --gateway "$OPENFAAS_GATEWAY"
success "Function 'parallel-sort' đã deploy."

# Kiểm tra function ready
info "Chờ function sẵn sàng..."
sleep 5
faas-cli describe parallel-sort --gateway "$OPENFAAS_GATEWAY" | grep -E "Status|Replicas"

cd ..

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 7: Cài Kafka Connector (OpenFaaS → Kafka trigger)
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 7: Kiểm tra / Cài Kafka Connector..."

if kubectl get deployment kafka-connector -n openfaas &>/dev/null; then
    warn "kafka-connector đã cài. Bỏ qua."
else
    info "Cài kafka-connector..."
    # Dùng arkade nếu có:
    # arkade install kafka-connector --broker-host kafka.kafka.svc.cluster.local:9092 --topics uit-lab3

    # Hoặc dùng manifest trực tiếp:
    cat <<EOF | kubectl apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-connector
  namespace: openfaas
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-connector
  template:
    metadata:
      labels:
        app: kafka-connector
    spec:
      containers:
      - name: kafka-connector
        image: ghcr.io/openfaas/kafka-connector:latest
        env:
        - name: gateway_url
          value: "http://gateway.openfaas.svc.cluster.local:8080"
        - name: broker_host
          value: "kafka.kafka.svc.cluster.local:9092"
        - name: topics
          value: "uit-lab3"
        - name: print_response
          value: "true"
        - name: print_request_body
          value: "false"
        - name: asynchronous_invocation
          value: "false"
        - name: username
          valueFrom:
            secretKeyRef:
              name: basic-auth
              key: basic-auth-user
        - name: password
          valueFrom:
            secretKeyRef:
              name: basic-auth
              key: basic-auth-password
EOF
    success "kafka-connector đã cài."
fi

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 8: Tạo ConfigMap cho producer script
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 8: Tạo ConfigMap 'producer-script'..."

kubectl create configmap producer-script \
    --from-file=chunker_producer.py=scripts/chunker_producer.py \
    -n kafka \
    --dry-run=client -o yaml | kubectl apply -f -
success "ConfigMap 'producer-script' đã tạo/cập nhật."

# ══════════════════════════════════════════════════════════════════════════════
# BƯỚC 9: Chạy Job producer
# ══════════════════════════════════════════════════════════════════════════════
info "Bước 9: Chạy Job producer..."

# Xóa Job cũ nếu còn tồn tại
if kubectl get job producer-job -n kafka &>/dev/null; then
    warn "Job 'producer-job' đã tồn tại. Đang xóa..."
    kubectl delete job producer-job -n kafka
    sleep 3
fi

kubectl apply -f kubernetes-manifests/job-producer.yaml
success "Job 'producer-job' đã tạo."

info "Theo dõi logs producer:"
info "  kubectl logs -f job/producer-job -n kafka"

echo ""
echo "══════════════════════════════════════════════════════════════"
echo -e "  ${GREEN}✓ Triển khai hoàn tất!${NC}"
echo ""
echo "  Các lệnh hữu ích sau deploy:"
echo "  • Xem Kafka pods:      kubectl get pods -n kafka"
echo "  • Xem function:        faas-cli list --gateway $OPENFAAS_GATEWAY"
echo "  • Xem function logs:   kubectl logs -n openfaas-fn -l faas_function=parallel-sort -f"
echo "  • Xem connector logs:  kubectl logs -n openfaas -l app=kafka-connector -f"
echo "  • Xem producer logs:   kubectl logs -f job/producer-job -n kafka"
echo "══════════════════════════════════════════════════════════════"
