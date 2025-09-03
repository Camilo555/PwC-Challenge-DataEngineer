#!/bin/bash

# PwC Data Engineering Platform Deployment Script
# This script provides automated deployment for different environments

set -e  # Exit on any error

# Default values
ENVIRONMENT="production"
DOCKER_REGISTRY=""
VERSION="latest"
CLEAN_DEPLOY=false
SKIP_TESTS=false
DATADOG_API_KEY=""
SCALE_WORKERS=2

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Display usage
show_help() {
    cat << EOF
PwC Data Engineering Platform Deployment Script

Usage: $0 [OPTIONS]

OPTIONS:
    -e, --environment ENV     Target environment (production, staging, development)
    -r, --registry URL        Docker registry URL
    -v, --version TAG         Version/tag to deploy
    -c, --clean               Clean deployment (removes existing containers)
    -s, --skip-tests          Skip pre-deployment tests
    -w, --workers NUM         Number of worker instances to scale
    --datadog-key KEY         DataDog API key for monitoring
    -h, --help                Show this help message

EXAMPLES:
    $0 -e production -v v1.2.3 --datadog-key=your_key_here
    $0 -e staging -c -w 1
    $0 -e development --skip-tests

EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -e|--environment)
                ENVIRONMENT="$2"
                shift 2
                ;;
            -r|--registry)
                DOCKER_REGISTRY="$2"
                shift 2
                ;;
            -v|--version)
                VERSION="$2"
                shift 2
                ;;
            -c|--clean)
                CLEAN_DEPLOY=true
                shift
                ;;
            -s|--skip-tests)
                SKIP_TESTS=true
                shift
                ;;
            -w|--workers)
                SCALE_WORKERS="$2"
                shift 2
                ;;
            --datadog-key)
                DATADOG_API_KEY="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Validate environment
validate_environment() {
    case $ENVIRONMENT in
        production|staging|development)
            log_info "Target environment: $ENVIRONMENT"
            ;;
        *)
            log_error "Invalid environment: $ENVIRONMENT"
            log_error "Valid environments: production, staging, development"
            exit 1
            ;;
    esac
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    # Check available disk space (minimum 10GB)
    available_space=$(df . | tail -1 | awk '{print $4}')
    if [ "$available_space" -lt 10485760 ]; then  # 10GB in KB
        log_warning "Low disk space detected. Ensure at least 10GB is available."
    fi
    
    log_success "Prerequisites check passed"
}

# Run tests
run_tests() {
    if [ "$SKIP_TESTS" = true ]; then
        log_warning "Skipping tests as requested"
        return
    fi
    
    log_info "Running pre-deployment tests..."
    
    # Build test image
    docker build -f deployment/Dockerfile.test -t pwc-data-platform:test .
    
    # Run tests
    if docker run --rm \
        -v "$(pwd)/data:/app/data" \
        -v "$(pwd)/config:/app/config" \
        -e PYTHONPATH=/app/src \
        pwc-data-platform:test \
        python -m pytest tests/ -v --tb=short; then
        log_success "All tests passed"
    else
        log_error "Tests failed. Deployment aborted."
        exit 1
    fi
}

# Build Docker images
build_images() {
    log_info "Building Docker images..."
    
    # Set image tag
    if [ -n "$DOCKER_REGISTRY" ]; then
        IMAGE_TAG="${DOCKER_REGISTRY}/pwc-data-platform:${VERSION}"
    else
        IMAGE_TAG="pwc-data-platform:${VERSION}"
    fi
    
    # Build main application image
    if docker build -f deployment/Dockerfile -t "$IMAGE_TAG" .; then
        log_success "Successfully built image: $IMAGE_TAG"
    else
        log_error "Failed to build Docker image"
        exit 1
    fi
    
    # Push to registry if specified
    if [ -n "$DOCKER_REGISTRY" ]; then
        log_info "Pushing image to registry..."
        if docker push "$IMAGE_TAG"; then
            log_success "Successfully pushed image to registry"
        else
            log_error "Failed to push image to registry"
            exit 1
        fi
    fi
}

# Clean existing deployment
clean_deployment() {
    if [ "$CLEAN_DEPLOY" = true ]; then
        log_info "Cleaning existing deployment..."
        
        # Stop and remove containers
        docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" down -v --remove-orphans
        
        # Remove unused images
        docker image prune -f
        
        log_success "Cleaned existing deployment"
    fi
}

# Deploy services
deploy_services() {
    log_info "Deploying services for $ENVIRONMENT environment..."
    
    # Set environment variables
    export DD_API_KEY="$DATADOG_API_KEY"
    export IMAGE_TAG="${IMAGE_TAG:-pwc-data-platform:${VERSION}}"
    
    # Create necessary directories
    mkdir -p data/{raw,processed,warehouse} logs config
    
    # Deploy using docker-compose
    if docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" up -d; then
        log_success "Services deployed successfully"
    else
        log_error "Failed to deploy services"
        exit 1
    fi
    
    # Scale workers if specified
    if [ "$SCALE_WORKERS" -gt 1 ]; then
        log_info "Scaling worker instances to $SCALE_WORKERS..."
        docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" up -d --scale spark-worker="$SCALE_WORKERS"
    fi
}

# Health checks
perform_health_checks() {
    log_info "Performing health checks..."
    
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        log_info "Health check attempt $attempt/$max_attempts..."
        
        # Check main application
        if curl -f http://localhost:8000/health &> /dev/null; then
            log_success "Main application is healthy"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            log_error "Health checks failed after $max_attempts attempts"
            log_info "Showing recent logs:"
            docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" logs --tail=50 pipeline
            exit 1
        fi
        
        sleep 10
        ((attempt++))
    done
    
    # Check other services
    local services=("redis" "rabbitmq" "prometheus")
    for service in "${services[@]}"; do
        if docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" ps "$service" | grep -q "Up"; then
            log_success "$service is running"
        else
            log_warning "$service may not be running properly"
        fi
    done
}

# Display deployment summary
show_deployment_summary() {
    log_success "Deployment completed successfully!"
    echo
    log_info "Deployment Summary:"
    echo "  Environment: $ENVIRONMENT"
    echo "  Version: $VERSION"
    echo "  Workers: $SCALE_WORKERS"
    echo
    log_info "Service URLs:"
    echo "  Main Application: http://localhost:8000"
    echo "  Grafana Dashboard: http://localhost:3000 (admin/admin123)"
    echo "  Prometheus: http://localhost:9090"
    echo "  RabbitMQ Management: http://localhost:15672 (admin/admin123)"
    echo "  Spark UI: http://localhost:8080"
    echo
    log_info "Useful commands:"
    echo "  View logs: docker-compose -f deployment/docker-compose.${ENVIRONMENT}.yml logs -f"
    echo "  Stop services: docker-compose -f deployment/docker-compose.${ENVIRONMENT}.yml down"
    echo "  Scale workers: docker-compose -f deployment/docker-compose.${ENVIRONMENT}.yml up -d --scale spark-worker=N"
}

# Rollback function
rollback_deployment() {
    log_error "Rolling back deployment..."
    docker-compose -f "deployment/docker-compose.${ENVIRONMENT}.yml" down
    log_success "Rollback completed"
}

# Trap errors and rollback
trap rollback_deployment ERR

# Main execution
main() {
    log_info "Starting PwC Data Engineering Platform deployment..."
    
    parse_args "$@"
    validate_environment
    check_prerequisites
    run_tests
    build_images
    clean_deployment
    deploy_services
    perform_health_checks
    show_deployment_summary
    
    log_success "Deployment completed successfully!"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi