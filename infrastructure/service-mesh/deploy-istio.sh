#!/bin/bash
# Istio Service Mesh Deployment Script
# Comprehensive deployment and configuration of Istio service mesh for enterprise data platform

set -euo pipefail

# Configuration
ISTIO_VERSION="1.19.0"
NAMESPACE="istio-system"
MESH_NAMESPACE="pwc-data-platform"
DOMAIN="pwc-dataplatform.com"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}"
    exit 1
}

info() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] INFO: $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    # Check kubectl
    if ! command -v kubectl &> /dev/null; then
        error "kubectl is not installed or not in PATH"
    fi

    # Check helm
    if ! command -v helm &> /dev/null; then
        error "helm is not installed or not in PATH"
    fi

    # Check cluster connectivity
    if ! kubectl cluster-info &> /dev/null; then
        error "Unable to connect to Kubernetes cluster"
    fi

    # Check cluster version
    K8S_VERSION=$(kubectl version --short | grep "Server Version" | awk '{print $3}' | sed 's/v//')
    MAJOR=$(echo $K8S_VERSION | cut -d. -f1)
    MINOR=$(echo $K8S_VERSION | cut -d. -f2)

    if [[ $MAJOR -lt 1 ]] || [[ $MAJOR -eq 1 && $MINOR -lt 23 ]]; then
        error "Kubernetes version must be 1.23 or higher. Current version: $K8S_VERSION"
    fi

    log "Prerequisites check passed"
}

# Download and install Istio
install_istio() {
    log "Installing Istio $ISTIO_VERSION..."

    # Download Istio
    if [[ ! -d "istio-$ISTIO_VERSION" ]]; then
        info "Downloading Istio $ISTIO_VERSION..."
        curl -L https://istio.io/downloadIstio | ISTIO_VERSION=$ISTIO_VERSION TARGET_ARCH=x86_64 sh -
    fi

    export PATH="$PWD/istio-$ISTIO_VERSION/bin:$PATH"

    # Verify istioctl
    if ! command -v istioctl &> /dev/null; then
        error "istioctl is not available after installation"
    fi

    # Pre-check cluster
    info "Running Istio pre-installation check..."
    istioctl x precheck || error "Istio pre-check failed"

    log "Istio CLI installed successfully"
}

# Create namespaces
create_namespaces() {
    log "Creating namespaces..."

    # Create istio-system namespace
    kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

    # Create application namespace
    kubectl create namespace $MESH_NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
    kubectl label namespace $MESH_NAMESPACE istio-injection=enabled --overwrite

    log "Namespaces created and labeled"
}

# Install Istio control plane
install_control_plane() {
    log "Installing Istio control plane..."

    # Apply the Istio operator configuration
    kubectl apply -f istio-configuration.yaml

    # Wait for control plane to be ready
    info "Waiting for Istio control plane to be ready..."
    kubectl wait --for=condition=Ready pod -l app=istiod -n $NAMESPACE --timeout=300s

    # Verify installation
    istioctl verify-install

    log "Istio control plane installed successfully"
}

# Install observability addons
install_observability() {
    log "Installing observability components..."

    # Add Prometheus
    kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.19/samples/addons/prometheus.yaml

    # Add Grafana
    kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.19/samples/addons/grafana.yaml

    # Add Jaeger
    kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.19/samples/addons/jaeger.yaml

    # Add Kiali
    kubectl apply -f https://raw.githubusercontent.com/istio/istio/release-1.19/samples/addons/kiali.yaml

    # Wait for components to be ready
    info "Waiting for observability components..."
    kubectl wait --for=condition=Available deployment/prometheus -n $NAMESPACE --timeout=300s
    kubectl wait --for=condition=Available deployment/grafana -n $NAMESPACE --timeout=300s
    kubectl wait --for=condition=Available deployment/jaeger -n $NAMESPACE --timeout=300s
    kubectl wait --for=condition=Available deployment/kiali -n $NAMESPACE --timeout=300s

    log "Observability components installed successfully"
}

# Create TLS certificates
create_certificates() {
    log "Creating TLS certificates..."

    # Create self-signed certificate for development
    # In production, use proper certificates from CA

    # Create private key
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
        -keyout tls.key -out tls.crt \
        -subj "/CN=$DOMAIN/O=$DOMAIN"

    # Create Kubernetes secret
    kubectl create secret tls pwc-tls-secret \
        --key=tls.key \
        --cert=tls.crt \
        -n $NAMESPACE \
        --dry-run=client -o yaml | kubectl apply -f -

    # Clean up files
    rm -f tls.key tls.crt

    log "TLS certificates created"
}

# Apply traffic management policies
apply_traffic_policies() {
    log "Applying traffic management policies..."

    # The policies are already in istio-configuration.yaml
    # This function can be used to apply additional policies

    # Apply any additional policies from separate files
    if [[ -d "policies" ]]; then
        for policy in policies/*.yaml; do
            if [[ -f "$policy" ]]; then
                info "Applying policy: $policy"
                kubectl apply -f "$policy"
            fi
        done
    fi

    log "Traffic management policies applied"
}

# Validate mesh configuration
validate_mesh() {
    log "Validating mesh configuration..."

    # Check proxy status
    info "Checking proxy status..."
    istioctl proxy-status

    # Analyze configuration
    info "Analyzing mesh configuration..."
    istioctl analyze -n $MESH_NAMESPACE

    # Check certificates
    info "Checking mTLS configuration..."
    kubectl exec -n $MESH_NAMESPACE deployment/fastapi-service -c istio-proxy -- \
        openssl s_client -showcerts -connect fastapi-service:8000 -servername fastapi-service < /dev/null || warn "mTLS validation skipped - service not ready"

    log "Mesh validation completed"
}

# Setup monitoring and alerting
setup_monitoring() {
    log "Setting up monitoring and alerting..."

    # Create ServiceMonitor for Prometheus
    cat <<EOF | kubectl apply -f -
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: istio-mesh-metrics
  namespace: $NAMESPACE
spec:
  selector:
    matchLabels:
      app: istiod
  endpoints:
  - port: http-monitoring
    interval: 15s
    path: /stats/prometheus
EOF

    # Create PrometheusRule for alerts
    cat <<EOF | kubectl apply -f -
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: istio-alerts
  namespace: $NAMESPACE
spec:
  groups:
  - name: istio-alerts
    rules:
    - alert: IstioControlPlaneDown
      expr: up{job="istiod"} == 0
      for: 5m
      labels:
        severity: critical
      annotations:
        summary: "Istio control plane is down"
        description: "Istio control plane has been down for more than 5 minutes"

    - alert: HighErrorRate
      expr: rate(istio_request_total{response_code=~"5.."}[5m]) > 0.1
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "High error rate detected"
        description: "Error rate is above 10% for {{ \$labels.destination_service_name }}"

    - alert: HighLatency
      expr: histogram_quantile(0.99, rate(istio_request_duration_milliseconds_bucket[5m])) > 1000
      for: 2m
      labels:
        severity: warning
      annotations:
        summary: "High latency detected"
        description: "99th percentile latency is above 1s for {{ \$labels.destination_service_name }}"
EOF

    log "Monitoring and alerting configured"
}

# Generate deployment report
generate_report() {
    log "Generating deployment report..."

    local report_file="istio-deployment-report-$(date +%Y%m%d-%H%M%S).txt"

    {
        echo "=== Istio Service Mesh Deployment Report ==="
        echo "Date: $(date)"
        echo "Istio Version: $ISTIO_VERSION"
        echo "Kubernetes Cluster: $(kubectl config current-context)"
        echo ""

        echo "=== Istio Components Status ==="
        kubectl get pods -n $NAMESPACE
        echo ""

        echo "=== Gateway Status ==="
        kubectl get gateway -n $MESH_NAMESPACE
        echo ""

        echo "=== Virtual Services ==="
        kubectl get virtualservice -n $MESH_NAMESPACE
        echo ""

        echo "=== Destination Rules ==="
        kubectl get destinationrule -n $MESH_NAMESPACE
        echo ""

        echo "=== Service Entries ==="
        kubectl get serviceentry -n $MESH_NAMESPACE
        echo ""

        echo "=== Authorization Policies ==="
        kubectl get authorizationpolicy -n $MESH_NAMESPACE
        echo ""

        echo "=== Peer Authentication ==="
        kubectl get peerauthentication -n $MESH_NAMESPACE
        echo ""

        echo "=== Istio Configuration Analysis ==="
        istioctl analyze -n $MESH_NAMESPACE 2>&1 || echo "Analysis completed with warnings/errors"
        echo ""

        echo "=== External IP Addresses ==="
        kubectl get svc istio-ingressgateway -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "LoadBalancer IP not available"
        echo ""

        echo "=== Access Information ==="
        echo "API Endpoint: https://api.$DOMAIN"
        echo "Monitoring Dashboard: https://monitoring.$DOMAIN"
        echo "Kiali Dashboard: kubectl port-forward -n $NAMESPACE svc/kiali 20001:20001"
        echo "Grafana Dashboard: kubectl port-forward -n $NAMESPACE svc/grafana 3000:3000"
        echo "Jaeger UI: kubectl port-forward -n $NAMESPACE svc/jaeger 16686:16686"
        echo ""

        echo "=== Next Steps ==="
        echo "1. Deploy your applications with Istio sidecar injection"
        echo "2. Configure DNS to point to the ingress gateway IP"
        echo "3. Set up proper TLS certificates for production"
        echo "4. Configure monitoring and alerting"
        echo "5. Test traffic management policies"

    } > $report_file

    info "Deployment report generated: $report_file"
}

# Cleanup function
cleanup() {
    if [[ "${1:-}" == "full" ]]; then
        warn "Performing full cleanup..."
        kubectl delete -f istio-configuration.yaml --ignore-not-found=true
        istioctl uninstall --purge -y || true
        kubectl delete namespace $NAMESPACE --ignore-not-found=true
        kubectl delete namespace $MESH_NAMESPACE --ignore-not-found=true
    fi
}

# Main deployment function
main() {
    local action="${1:-deploy}"

    case $action in
        deploy)
            log "Starting Istio service mesh deployment..."
            check_prerequisites
            install_istio
            create_namespaces
            install_control_plane
            install_observability
            create_certificates
            apply_traffic_policies
            validate_mesh
            setup_monitoring
            generate_report
            log "Istio service mesh deployment completed successfully!"
            ;;
        validate)
            log "Validating existing Istio deployment..."
            validate_mesh
            ;;
        cleanup)
            cleanup "${2:-}"
            ;;
        report)
            generate_report
            ;;
        *)
            echo "Usage: $0 {deploy|validate|cleanup|report}"
            echo "  deploy  - Full deployment of Istio service mesh"
            echo "  validate - Validate existing deployment"
            echo "  cleanup - Remove Istio (use 'cleanup full' for complete removal)"
            echo "  report  - Generate deployment report"
            exit 1
            ;;
    esac
}

# Handle script interruption
trap 'error "Deployment interrupted"' INT TERM

# Run main function
main "$@"