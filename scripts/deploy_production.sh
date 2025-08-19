#!/bin/bash

# Deploy to Production Environment
# This script deploys the application to a production environment with additional safety checks

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
ENVIRONMENT="production"
COMPOSE_FILE="docker-compose.production.yml"
ENV_FILE=".env.production"
BACKUP_DIR="/backups/$(date +%Y%m%d-%H%M%S)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[DEPLOY]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
}

# Check if running with required permissions and safety checks
check_production_requirements() {
    log "Checking production deployment requirements..."
    
    # Check if this is actually a production environment
    if [ "${ENVIRONMENT}" != "production" ]; then
        error "This script is only for production deployments"
        exit 1
    fi
    
    # Check Docker
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
    
    # Check required environment variables
    local required_vars=(
        "POSTGRES_PASSWORD"
        "SECRET_KEY"
        "BASIC_AUTH_PASSWORD"
        "TYPESENSE_API_KEY"
    )
    
    for var in "${required_vars[@]}"; do
        if [ -z "${!var:-}" ]; then
            error "Required environment variable $var is not set"
            exit 1
        fi
    done
    
    # Check backup directory exists
    if [ ! -d "$(dirname "$BACKUP_DIR")" ]; then
        warning "Backup directory $(dirname "$BACKUP_DIR") does not exist, creating..."
        mkdir -p "$(dirname "$BACKUP_DIR")"
    fi
    
    success "Production requirements check passed"
}

# Create production environment file
create_production_env() {
    log "Creating production environment configuration..."
    
    # Check if production env file already exists
    if [ -f "$PROJECT_ROOT/$ENV_FILE" ]; then
        warning "Production environment file already exists, creating backup..."
        cp "$PROJECT_ROOT/$ENV_FILE" "$PROJECT_ROOT/$ENV_FILE.backup.$(date +%Y%m%d-%H%M%S)"
    fi
    
    cat > "$PROJECT_ROOT/$ENV_FILE" << EOF
# Production Environment Configuration
ENVIRONMENT=production
DATABASE_TYPE=postgresql
POSTGRES_DB=retail_production
POSTGRES_USER=postgres
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
POSTGRES_PORT=5432

# Redis Configuration
REDIS_PORT=6379

# API Configuration
SECRET_KEY=${SECRET_KEY}
BASIC_AUTH_PASSWORD=${BASIC_AUTH_PASSWORD}

# Typesense Configuration
TYPESENSE_API_KEY=${TYPESENSE_API_KEY}

# Airflow Configuration
AIRFLOW_FERNET_KEY=${AIRFLOW_FERNET_KEY}
AIRFLOW_SECRET_KEY=${AIRFLOW_SECRET_KEY}

# Monitoring Configuration
GRAFANA_PASSWORD=${GRAFANA_PASSWORD}

# External Services
ENABLE_EXTERNAL_ENRICHMENT=true
ENABLE_VECTOR_SEARCH=true
ENABLE_MONITORING=true

# Production specific settings
ENABLE_DEBUG=false
LOG_LEVEL=INFO
WORKER_PROCESSES=4
MAX_CONNECTIONS=1000
EOF

    # Set secure permissions on env file
    chmod 600 "$PROJECT_ROOT/$ENV_FILE"
    
    success "Production environment configuration created"
}

# Create backup of current state
create_backup() {
    log "Creating backup of current production state..."
    
    mkdir -p "$BACKUP_DIR"
    
    # Backup database
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps postgres | grep -q "Up"; then
        log "Backing up database..."
        docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" exec -T postgres pg_dump -U postgres retail_production > "$BACKUP_DIR/database_backup.sql"
        
        if [ $? -eq 0 ]; then
            success "Database backup created: $BACKUP_DIR/database_backup.sql"
        else
            error "Database backup failed"
            return 1
        fi
    else
        warning "PostgreSQL container not running, skipping database backup"
    fi
    
    # Backup application data
    if [ -d "$PROJECT_ROOT/data" ]; then
        log "Backing up application data..."
        tar -czf "$BACKUP_DIR/data_backup.tar.gz" -C "$PROJECT_ROOT" data/
        success "Application data backup created: $BACKUP_DIR/data_backup.tar.gz"
    fi
    
    # Backup current docker-compose configuration
    cp "$PROJECT_ROOT/$COMPOSE_FILE" "$BACKUP_DIR/"
    if [ -f "$PROJECT_ROOT/$ENV_FILE" ]; then
        cp "$PROJECT_ROOT/$ENV_FILE" "$BACKUP_DIR/"
    fi
    
    # Create backup metadata
    cat > "$BACKUP_DIR/backup_info.txt" << EOF
Backup created: $(date)
Environment: $ENVIRONMENT
Git commit: $(git rev-parse HEAD 2>/dev/null || echo "Not available")
Docker images:
$(docker images --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}" | head -10)
EOF
    
    success "Backup completed: $BACKUP_DIR"
}

# Pre-deployment checks and validation
pre_deployment_checks() {
    log "Running pre-deployment checks..."
    
    # Check system resources
    log "Checking system resources..."
    
    # Check memory
    AVAILABLE_MEMORY=$(free -m | awk 'NR==2{print $7}')
    REQUIRED_MEMORY=4096  # 4GB
    
    if [ "$AVAILABLE_MEMORY" -lt "$REQUIRED_MEMORY" ]; then
        warning "Low memory: ${AVAILABLE_MEMORY}MB available, ${REQUIRED_MEMORY}MB recommended"
    fi
    
    # Check disk space
    AVAILABLE_SPACE=$(df "$PROJECT_ROOT" | awk 'NR==2 {print $4}')
    REQUIRED_SPACE=10000000  # 10GB in KB
    
    if [ "$AVAILABLE_SPACE" -lt "$REQUIRED_SPACE" ]; then
        error "Insufficient disk space: ${AVAILABLE_SPACE}KB available, ${REQUIRED_SPACE}KB required"
        return 1
    fi
    
    # Check for required external services
    log "Checking external service dependencies..."
    
    # Test internet connectivity for external APIs (if enabled)
    if [ "${ENABLE_EXTERNAL_ENRICHMENT:-false}" = "true" ]; then
        if ! curl -s --max-time 10 https://api.exchangerate-api.com/v4/latest/USD > /dev/null; then
            warning "External API connectivity test failed"
        fi
    fi
    
    success "Pre-deployment checks completed"
}

# Pull and prepare images
prepare_production_images() {
    log "Preparing production Docker images..."
    
    cd "$PROJECT_ROOT"
    
    # Pull latest images from registry
    log "Pulling latest images from registry..."
    docker-compose -f "$COMPOSE_FILE" pull postgres redis typesense prometheus grafana
    
    # Build production images
    log "Building production images..."
    docker-compose -f "$COMPOSE_FILE" build --no-cache api etl-spark dagster
    
    # Tag images with production tags
    IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
    
    # Remove old unused images to free space
    log "Cleaning up old Docker images..."
    docker image prune -f
    
    success "Production images prepared"
}

# Rolling deployment with zero downtime
rolling_deployment() {
    log "Starting rolling deployment..."
    
    cd "$PROJECT_ROOT"
    
    # Phase 1: Update database and core services
    log "Phase 1: Updating core services..."
    
    # Start new database if not running
    if ! docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" ps postgres | grep -q "Up"; then
        docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d postgres
        sleep 30
    fi
    
    # Start Redis and Typesense
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d redis typesense
    sleep 15
    
    # Phase 2: Rolling update of API service
    log "Phase 2: Rolling update of API service..."
    
    # Scale up with new version
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --scale api=2 api
    sleep 30
    
    # Health check new instances
    local health_check_passed=false
    for i in {1..10}; do
        if curl -f -s http://localhost:8000/api/v1/health > /dev/null; then
            health_check_passed=true
            break
        fi
        log "Health check attempt $i/10..."
        sleep 10
    done
    
    if [ "$health_check_passed" = false ]; then
        error "Health check failed for new API instances"
        return 1
    fi
    
    # Scale down to 1 instance (remove old)
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up -d --scale api=1 api
    
    # Phase 3: Update other services
    log "Phase 3: Updating ETL and orchestration services..."
    
    # Update ETL services (these can have downtime)
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile dagster up -d dagster
    
    # Phase 4: Update monitoring
    log "Phase 4: Updating monitoring services..."
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile monitoring up -d prometheus grafana
    
    success "Rolling deployment completed"
}

# Comprehensive health checks
production_health_checks() {
    log "Running comprehensive production health checks..."
    
    local max_attempts=60
    local attempt=1
    
    # Check API health
    log "Checking API health..."
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s http://localhost:8000/api/v1/health > /dev/null; then
            success "API health check passed"
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            error "API health check failed after $max_attempts attempts"
            return 1
        fi
        
        log "Attempt $attempt/$max_attempts: API not ready, waiting..."
        sleep 5
        ((attempt++))
    done
    
    # Check detailed API health
    log "Checking detailed API health..."
    local api_health=$(curl -s -u "admin:${BASIC_AUTH_PASSWORD}" http://localhost:8000/api/v1/health/detailed)
    if echo "$api_health" | grep -q "healthy"; then
        success "Detailed API health check passed"
    else
        warning "Detailed API health check returned warnings"
    fi
    
    # Check database health
    log "Checking database health..."
    if docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" exec -T postgres pg_isready -U postgres > /dev/null; then
        success "Database health check passed"
    else
        error "Database health check failed"
        return 1
    fi
    
    # Check database connectivity from API
    log "Testing database connectivity from API..."
    if curl -f -s -u "admin:${BASIC_AUTH_PASSWORD}" http://localhost:8000/api/v1/sales | grep -q "data\|total"; then
        success "Database connectivity from API verified"
    else
        warning "Database connectivity test from API failed"
    fi
    
    # Check Typesense
    log "Checking Typesense health..."
    if curl -f -s http://localhost:8108/health > /dev/null; then
        success "Typesense health check passed"
    else
        warning "Typesense health check failed"
    fi
    
    # Check monitoring services
    if [ "${ENABLE_MONITORING:-true}" = "true" ]; then
        log "Checking monitoring services..."
        
        if curl -f -s http://localhost:9090/-/healthy > /dev/null; then
            success "Prometheus health check passed"
        else
            warning "Prometheus health check failed"
        fi
        
        if curl -f -s http://localhost:3001/api/health > /dev/null; then
            success "Grafana health check passed"
        else
            warning "Grafana health check failed"
        fi
    fi
    
    success "Production health checks completed"
}

# Run production smoke tests
production_smoke_tests() {
    log "Running production smoke tests..."
    
    # Test critical business endpoints
    log "Testing critical business endpoints..."
    
    local endpoints=(
        "/api/v1/sales/summary"
        "/api/v1/customers/summary"
        "/api/v1/products/summary"
        "/api/v1/monitoring/metrics"
    )
    
    for endpoint in "${endpoints[@]}"; do
        if curl -f -s -u "admin:${BASIC_AUTH_PASSWORD}" "http://localhost:8000$endpoint" > /dev/null; then
            success "Endpoint $endpoint test passed"
        else
            warning "Endpoint $endpoint test failed"
        fi
    done
    
    # Test ETL pipeline (dry run)
    log "Testing ETL pipeline..."
    if docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" run --rm etl-spark python scripts/run_etl.py --dry-run; then
        success "ETL pipeline test passed"
    else
        warning "ETL pipeline test failed"
    fi
    
    success "Production smoke tests completed"
}

# Setup monitoring and alerting
setup_monitoring() {
    log "Setting up production monitoring and alerting..."
    
    # Start monitoring stack
    docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" --profile monitoring up -d
    
    # Wait for services to be ready
    sleep 30
    
    # Configure Grafana dashboards (if not already done)
    # This would typically be done through provisioning or API calls
    
    success "Monitoring setup completed"
}

# Rollback function
rollback_deployment() {
    error "Deployment failed, initiating rollback..."
    
    if [ -d "$BACKUP_DIR" ]; then
        log "Rolling back to previous state..."
        
        # Stop current services
        docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" --env-file "$ENV_FILE" down
        
        # Restore configuration
        if [ -f "$BACKUP_DIR/$COMPOSE_FILE" ]; then
            cp "$BACKUP_DIR/$COMPOSE_FILE" "$PROJECT_ROOT/"
        fi
        
        if [ -f "$BACKUP_DIR/$ENV_FILE" ]; then
            cp "$BACKUP_DIR/$ENV_FILE" "$PROJECT_ROOT/"
        fi
        
        # Restore database if needed
        if [ -f "$BACKUP_DIR/database_backup.sql" ]; then
            log "Restoring database..."
            docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" up -d postgres
            sleep 30
            docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" exec -T postgres psql -U postgres -d retail_production < "$BACKUP_DIR/database_backup.sql"
        fi
        
        # Start services with old configuration
        docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" --env-file "$ENV_FILE" up -d
        
        success "Rollback completed"
    else
        error "No backup found for rollback"
    fi
}

# Cleanup function
cleanup() {
    log "Performing cleanup..."
    
    # Remove temporary files
    rm -f /tmp/deploy_production_*
    
    # Clean up old Docker images (keep last 3 versions)
    docker images | grep "retail-etl-pipeline" | tail -n +4 | awk '{print $3}' | xargs -r docker rmi 2>/dev/null || true
    
    log "Cleanup completed"
}

# Send deployment notification
send_notification() {
    local status=$1
    local message=$2
    
    log "Sending deployment notification..."
    
    # Send to Slack if webhook is configured
    if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
        local color="good"
        if [ "$status" != "success" ]; then
            color="danger"
        fi
        
        curl -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"Production Deployment $status\", \"attachments\":[{\"color\":\"$color\", \"text\":\"$message\"}]}" \
            "$SLACK_WEBHOOK_URL" 2>/dev/null || warning "Failed to send Slack notification"
    fi
}

# Main deployment function
main() {
    log "Starting production deployment..."
    log "Project root: $PROJECT_ROOT"
    log "Environment: $ENVIRONMENT"
    log "Backup directory: $BACKUP_DIR"
    
    # Trap to ensure cleanup and rollback on failure
    trap 'rollback_deployment; cleanup; send_notification "failed" "Production deployment failed and rollback initiated"' ERR
    trap cleanup EXIT
    
    # Confirmation prompt for production
    read -p "⚠️  This will deploy to PRODUCTION. Are you sure? (yes/no): " -r
    if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
        log "Deployment cancelled by user"
        exit 0
    fi
    
    # Run deployment steps
    check_production_requirements
    create_backup
    create_production_env
    pre_deployment_checks
    prepare_production_images
    rolling_deployment
    production_health_checks
    production_smoke_tests
    setup_monitoring
    
    # Remove error trap since we succeeded
    trap cleanup EXIT
    
    success "🎉 Production deployment completed successfully!"
    log "Deployment completed at: $(date)"
    log "Backup location: $BACKUP_DIR"
    log ""
    log "Access the application at:"
    log "  - API: http://localhost:8000"
    log "  - API Health: http://localhost:8000/api/v1/health"
    log "  - Dagster: http://localhost:3000"
    log "  - Prometheus: http://localhost:9090"
    log "  - Grafana: http://localhost:3001"
    
    # Send success notification
    send_notification "success" "Production deployment completed successfully at $(date)"
    
    log ""
    log "To view logs: docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE logs -f"
    log "To check status: docker-compose -f $COMPOSE_FILE --env-file $ENV_FILE ps"
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi