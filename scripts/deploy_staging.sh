#!/bin/bash

# Deploy to Staging Environment
# This script deploys the application to a staging environment

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENVIRONMENT="staging"
COMPOSE_FILE="docker-compose.production.yml"
ENV_FILE=".env.staging"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[DEPLOY]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running with required permissions
check_permissions() {
    log "Checking deployment permissions..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        error "Docker Compose is not installed or not in PATH"
        exit 1
    fi
    
    # Test docker access
    if ! docker info &> /dev/null; then
        error "Cannot access Docker daemon. Please check Docker is running and you have permissions."
        exit 1
    fi
    
    success "Permissions check passed"
}

# Create staging environment file
create_staging_env() {
    log "Creating staging environment configuration..."
    
    cat > "$PROJECT_ROOT/$ENV_FILE" << EOF
# Staging Environment Configuration
ENVIRONMENT=staging
DATABASE_TYPE=postgresql
POSTGRES_DB=retail_staging
POSTGRES_USER=postgres
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-staging_password_$(openssl rand -hex 8)}
POSTGRES_PORT=5432

# Redis Configuration
REDIS_PORT=6379

# API Configuration
SECRET_KEY=${SECRET_KEY:-$(openssl rand -hex 32)}
BASIC_AUTH_PASSWORD=${BASIC_AUTH_PASSWORD:-staging_password_$(openssl rand -hex 8)}

# Typesense Configuration
TYPESENSE_API_KEY=${TYPESENSE_API_KEY:-$(openssl rand -hex 16)}

# Airflow Configuration (if using)
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY:-$(openssl rand -base64 32)}
AIRFLOW_SECRET_KEY=${AIRFLOW_SECRET_KEY:-$(openssl rand -hex 32)}

# Monitoring Configuration
GRAFANA_PASSWORD=${GRAFANA_PASSWORD:-staging_grafana_$(openssl rand -hex 8)}

# External Services (set to false for staging)
ENABLE_EXTERNAL_ENRICHMENT=false
ENABLE_VECTOR_SEARCH=true
ENABLE_MONITORING=true
EOF

    success "Staging environment configuration created"
}

# Pre-deployment health checks
pre_deployment_checks() {
    log "Running pre-deployment checks..."
    
    # Check disk space
    AVAILABLE_SPACE=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    REQUIRED_SPACE=5000000  # 5GB in KB
    
    if [ "$AVAILABLE_SPACE" -lt "$REQUIRED_SPACE" ]; then
        warning "Low disk space: ${AVAILABLE_SPACE}KB available, ${REQUIRED_SPACE}KB recommended"
    fi
    
    # Check if staging is already running
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps | grep -q "Up"; then
        warning "Some services are already running. They will be updated."
    fi
    
    success "Pre-deployment checks completed"
}

# Build and pull images
prepare_images() {
    log "Preparing Docker images..."
    
    cd "$PROJECT_ROOT"
    
    # Pull external images
    log "Pulling external images..."
    docker-compose -f "$COMPOSE_FILE" pull postgres redis typesense prometheus grafana
    
    # Build application images
    log "Building application images..."
    docker-compose -f "$COMPOSE_FILE" build api etl-spark dagster
    
    success "Docker images prepared"
}

# Deploy services
deploy_services() {
    log "Deploying services to staging..."
    
    cd "$PROJECT_ROOT"
    
    # Stop existing services
    log "Stopping existing services..."
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down
    
    # Start core services first
    log "Starting core services..."
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d postgres redis typesense
    
    # Wait for core services to be ready
    log "Waiting for core services to be ready..."
    sleep 30
    
    # Start application services
    log "Starting application services..."
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d api
    
    # Start orchestration services (optional profiles)
    if [ "${ENABLE_DAGSTER:-false}" = "true" ]; then
        log "Starting Dagster orchestration..."
        docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile dagster up -d dagster
    fi
    
    # Start monitoring services (optional)
    if [ "${ENABLE_MONITORING:-true}" = "true" ]; then
        log "Starting monitoring services..."
        docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile monitoring up -d prometheus grafana
    fi
    
    success "Services deployed successfully"
}

# Run health checks
health_checks() {
    log "Running post-deployment health checks..."
    
    # Wait for services to be fully ready
    sleep 45
    
    # Check API health
    log "Checking API health..."
    max_attempts=30
    attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s http://localhost:8000/api/v1/health > /dev/null; then
            success "API is healthy"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            error "API health check failed after $max_attempts attempts"
            return 1
        fi
        
        log "Attempt $attempt/$max_attempts: API not ready yet, waiting..."
        sleep 10
        ((attempt++))
    done
    
    # Check database connectivity
    log "Checking database connectivity..."
    if docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres pg_isready -U postgres > /dev/null; then
        success "Database is healthy"
    else
        error "Database health check failed"
        return 1
    fi
    
    # Check Typesense
    log "Checking Typesense..."
    if curl -f -s http://localhost:8108/health > /dev/null; then
        success "Typesense is healthy"
    else
        warning "Typesense health check failed"
    fi
    
    success "Health checks completed"
}

# Run integration tests
run_integration_tests() {
    log "Running integration tests..."
    
    # Test basic API endpoints
    log "Testing API endpoints..."
    
    # Test health endpoint
    if curl -f -s http://localhost:8000/api/v1/health; then
        success "Health endpoint test passed"
    else
        error "Health endpoint test failed"
        return 1
    fi
    
    # Test authenticated endpoint (if basic auth is configured)
    if [ -n "${BASIC_AUTH_PASSWORD:-}" ]; then
        if curl -f -s -u "admin:${BASIC_AUTH_PASSWORD}" http://localhost:8000/api/v1/sales > /dev/null; then
            success "Authenticated endpoint test passed"
        else
            warning "Authenticated endpoint test failed"
        fi
    fi
    
    success "Integration tests completed"
}

# Cleanup function
cleanup() {
    log "Performing cleanup..."
    
    # Remove temporary files
    rm -f /tmp/deploy_staging_*
    
    log "Cleanup completed"
}

# Main deployment function
main() {
    log "Starting staging deployment..."
    log "Project root: $PROJECT_ROOT"
    log "Environment: $ENVIRONMENT"
    
    # Trap to ensure cleanup runs on exit
    trap cleanup EXIT
    
    # Run deployment steps
    check_permissions
    create_staging_env
    pre_deployment_checks
    prepare_images
    deploy_services
    health_checks
    run_integration_tests
    
    success "🎉 Staging deployment completed successfully!"
    log "Access the application at:"
    log "  - API: http://localhost:8000"
    log "  - API Health: http://localhost:8000/api/v1/health"
    log "  - Typesense: http://localhost:8108"
    
    if [ "${ENABLE_MONITORING:-true}" = "true" ]; then
        log "  - Prometheus: http://localhost:9090"
        log "  - Grafana: http://localhost:3001"
    fi
    
    if [ "${ENABLE_DAGSTER:-false}" = "true" ]; then
        log "  - Dagster: http://localhost:3000"
    fi
    
    log ""
    log "To view logs: docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE logs -f"
    log "To stop services: docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE down"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi