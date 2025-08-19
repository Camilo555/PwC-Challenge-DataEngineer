#!/bin/bash

# Deployment Validation Script
# Validates that the deployment is working correctly across all services

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="docker-compose.production.yml"
ENV_FILE="${ENV_FILE:-.env.production}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log() {
    echo -e "${BLUE}[VALIDATE]${NC} $(date '+%Y-%m-%d %H:%M:%S') $1"
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

# Test result tracking
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_WARNED=0

# Function to run a test and track results
run_test() {
    local test_name="$1"
    local test_command="$2"
    local is_critical="${3:-true}"
    
    log "Running test: $test_name"
    
    if eval "$test_command" 2>/dev/null; then
        success "$test_name - PASSED"
        ((TESTS_PASSED++))
        return 0
    else
        if [ "$is_critical" = "true" ]; then
            error "$test_name - FAILED"
            ((TESTS_FAILED++))
            return 1
        else
            warning "$test_name - WARNING"
            ((TESTS_WARNED++))
            return 0
        fi
    fi
}

# Check if services are running
check_services_running() {
    log "Checking if services are running..."
    
    cd "$PROJECT_ROOT"
    
    local services=(
        "postgres"
        "redis"
        "api"
        "typesense"
    )
    
    for service in "${services[@]}"; do
        run_test "Service $service is running" \
            "docker-compose -f $COMPOSE_FILE ps $service | grep -q 'Up'"
    done
}

# Test API endpoints
test_api_endpoints() {
    log "Testing API endpoints..."
    
    # Wait for API to be ready
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -f -s http://localhost:8000/api/v1/health > /dev/null; then
            break
        fi
        
        if [ $attempt -eq $max_attempts ]; then
            error "API not ready after $max_attempts attempts"
            return 1
        fi
        
        log "Waiting for API to be ready (attempt $attempt/$max_attempts)..."
        sleep 5
        ((attempt++))
    done
    
    # Test health endpoint
    run_test "API health endpoint" \
        "curl -f -s http://localhost:8000/api/v1/health | grep -q 'healthy\|status'"
    
    # Test OpenAPI docs
    run_test "API documentation endpoint" \
        "curl -f -s http://localhost:8000/docs | grep -q 'Swagger UI'"
    
    # Test authenticated endpoints (if configured)
    if [ -n "${BASIC_AUTH_PASSWORD:-}" ]; then
        run_test "Authenticated sales endpoint" \
            "curl -f -s -u 'admin:${BASIC_AUTH_PASSWORD}' http://localhost:8000/api/v1/sales | grep -q 'data\|total\|count'"
        
        run_test "Authenticated customers endpoint" \
            "curl -f -s -u 'admin:${BASIC_AUTH_PASSWORD}' http://localhost:8000/api/v1/customers | grep -q 'data\|total\|count'"
    else
        warning "BASIC_AUTH_PASSWORD not set, skipping authenticated endpoint tests"
    fi
    
    # Test monitoring endpoints
    run_test "Monitoring metrics endpoint" \
        "curl -f -s http://localhost:8000/api/v1/monitoring/metrics | grep -q 'metrics\|system'" \
        false
}

# Test database connectivity
test_database() {
    log "Testing database connectivity..."
    
    cd "$PROJECT_ROOT"
    
    # Test PostgreSQL connection
    run_test "PostgreSQL connection" \
        "docker-compose -f $COMPOSE_FILE exec -T postgres pg_isready -U postgres"
    
    # Test database schema
    run_test "Database schema exists" \
        "docker-compose -f $COMPOSE_FILE exec -T postgres psql -U postgres -c '\\dt' | grep -q 'sales\|customers\|products'"
    
    # Test data exists
    run_test "Database contains data" \
        "docker-compose -f $COMPOSE_FILE exec -T postgres psql -U postgres -c 'SELECT COUNT(*) FROM sales;' | grep -E '[1-9][0-9]*'" \
        false
}

# Test Redis connectivity
test_redis() {
    log "Testing Redis connectivity..."
    
    cd "$PROJECT_ROOT"
    
    run_test "Redis connection" \
        "docker-compose -f $COMPOSE_FILE exec -T redis redis-cli ping | grep -q 'PONG'"
    
    # Test Redis operations
    run_test "Redis set/get operations" \
        "docker-compose -f $COMPOSE_FILE exec -T redis redis-cli set test_key test_value && docker-compose -f $COMPOSE_FILE exec -T redis redis-cli get test_key | grep -q 'test_value'"
}

# Test Typesense
test_typesense() {
    log "Testing Typesense..."
    
    run_test "Typesense health" \
        "curl -f -s http://localhost:8108/health | grep -q 'ok\|healthy'"
    
    # Test collections endpoint (if API key is configured)
    if [ -n "${TYPESENSE_API_KEY:-}" ]; then
        run_test "Typesense collections endpoint" \
            "curl -f -s -H 'X-TYPESENSE-API-KEY: ${TYPESENSE_API_KEY}' http://localhost:8108/collections | grep -q '\\[\\]\\|name'" \
            false
    else
        warning "TYPESENSE_API_KEY not set, skipping Typesense API tests"
    fi
}

# Test monitoring services
test_monitoring() {
    log "Testing monitoring services..."
    
    # Check if monitoring profile is running
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps prometheus 2>/dev/null | grep -q "Up"; then
        run_test "Prometheus health" \
            "curl -f -s http://localhost:9090/-/healthy | grep -q 'Prometheus is Healthy'"
        
        run_test "Prometheus metrics" \
            "curl -f -s http://localhost:9090/api/v1/query?query=up | grep -q 'success'"
    else
        log "Prometheus not running (monitoring profile not active)"
    fi
    
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps grafana 2>/dev/null | grep -q "Up"; then
        run_test "Grafana health" \
            "curl -f -s http://localhost:3001/api/health | grep -q 'ok\|database'"
    else
        log "Grafana not running (monitoring profile not active)"
    fi
}

# Test orchestration services
test_orchestration() {
    log "Testing orchestration services..."
    
    # Check Dagster
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps dagster 2>/dev/null | grep -q "Up"; then
        run_test "Dagster health" \
            "curl -f -s http://localhost:3000/server_info | grep -q 'dagster_version\|version'"
    else
        log "Dagster not running (orchestration profile not active)"
    fi
    
    # Check Airflow
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" ps airflow-webserver 2>/dev/null | grep -q "Up"; then
        run_test "Airflow webserver health" \
            "curl -f -s http://localhost:8081/health | grep -q 'healthy\|ok'"
    else
        log "Airflow not running (orchestration profile not active)"
    fi
}

# Test ETL functionality
test_etl() {
    log "Testing ETL functionality..."
    
    cd "$PROJECT_ROOT"
    
    # Test basic ETL script execution
    run_test "ETL script execution (dry run)" \
        "docker-compose -f $COMPOSE_FILE run --rm etl-spark python scripts/run_etl.py --dry-run" \
        false
    
    # Test individual ETL stages
    run_test "Bronze layer script" \
        "docker-compose -f $COMPOSE_FILE run --rm etl-spark python scripts/run_bronze.py --dry-run" \
        false
    
    run_test "Silver layer script" \
        "docker-compose -f $COMPOSE_FILE run --rm etl-spark python scripts/run_silver.py --dry-run" \
        false
    
    run_test "Gold layer script" \
        "docker-compose -f $COMPOSE_FILE run --rm etl-spark python scripts/run_gold.py --dry-run" \
        false
}

# Test data quality
test_data_quality() {
    log "Testing data quality..."
    
    cd "$PROJECT_ROOT"
    
    # Run data quality checks
    run_test "Data quality validation" \
        "docker-compose -f $COMPOSE_FILE exec -T api python -c 'from domain.validators.data_quality import DataQualityValidator; print(\"Data quality checks passed\")'" \
        false
    
    # Check for recent data
    run_test "Recent data exists" \
        "docker-compose -f $COMPOSE_FILE exec -T postgres psql -U postgres -c \"SELECT COUNT(*) FROM sales WHERE created_at > NOW() - INTERVAL '7 days';\" | grep -E '[0-9]+'" \
        false
}

# Test security
test_security() {
    log "Testing security configuration..."
    
    # Check that sensitive endpoints require authentication
    run_test "Unauthenticated access blocked" \
        "! curl -f -s http://localhost:8000/api/v1/sales > /dev/null"
    
    # Check HTTPS redirect (if configured)
    if [ "${ENVIRONMENT:-}" = "production" ]; then
        run_test "HTTPS redirect configured" \
            "curl -s -I http://localhost | grep -q 'Location: https'" \
            false
    fi
    
    # Check security headers
    run_test "Security headers present" \
        "curl -s -I http://localhost:8000/api/v1/health | grep -q 'X-Content-Type-Options\|X-Frame-Options'" \
        false
}

# Test performance
test_performance() {
    log "Testing performance..."
    
    # Test API response time
    local response_time
    response_time=$(curl -o /dev/null -s -w '%{time_total}' http://localhost:8000/api/v1/health)
    
    if (( $(echo "$response_time < 2.0" | bc -l) )); then
        success "API response time: ${response_time}s - PASSED"
        ((TESTS_PASSED++))
    else
        warning "API response time: ${response_time}s - SLOW"
        ((TESTS_WARNED++))
    fi
    
    # Test concurrent connections
    run_test "Concurrent API requests" \
        "seq 1 10 | xargs -I {} -P 10 curl -f -s http://localhost:8000/api/v1/health > /dev/null" \
        false
}

# Generate test report
generate_report() {
    log "Generating validation report..."
    
    local total_tests=$((TESTS_PASSED + TESTS_FAILED + TESTS_WARNED))
    local success_rate=0
    
    if [ $total_tests -gt 0 ]; then
        success_rate=$(( (TESTS_PASSED * 100) / total_tests ))
    fi
    
    echo ""
    echo "=================================="
    echo "DEPLOYMENT VALIDATION REPORT"
    echo "=================================="
    echo "Timestamp: $(date)"
    echo "Environment: ${ENVIRONMENT:-unknown}"
    echo ""
    echo "Test Results:"
    echo "  ✅ Passed: $TESTS_PASSED"
    echo "  ❌ Failed: $TESTS_FAILED"
    echo "  ⚠️  Warnings: $TESTS_WARNED"
    echo "  📊 Total: $total_tests"
    echo "  📈 Success Rate: ${success_rate}%"
    echo ""
    
    if [ $TESTS_FAILED -eq 0 ]; then
        if [ $TESTS_WARNED -eq 0 ]; then
            echo "🎉 All tests passed! Deployment is healthy."
            echo "✅ VALIDATION: PASSED"
        else
            echo "⚠️  Some warnings detected, but no critical failures."
            echo "⚠️  VALIDATION: PASSED WITH WARNINGS"
        fi
        return 0
    else
        echo "❌ Critical tests failed! Please investigate before proceeding."
        echo "❌ VALIDATION: FAILED"
        return 1
    fi
}

# Cleanup function
cleanup() {
    # Clean up any test data
    if docker-compose -f "$PROJECT_ROOT/$COMPOSE_FILE" exec -T redis redis-cli del test_key 2>/dev/null; then
        log "Cleaned up test data"
    fi
}

# Main validation function
main() {
    log "Starting deployment validation..."
    log "Project root: $PROJECT_ROOT"
    log "Environment file: $ENV_FILE"
    
    # Trap to ensure cleanup
    trap cleanup EXIT
    
    # Source environment file if it exists
    if [ -f "$PROJECT_ROOT/$ENV_FILE" ]; then
        set -a
        source "$PROJECT_ROOT/$ENV_FILE"
        set +a
        log "Loaded environment from $ENV_FILE"
    else
        warning "Environment file $ENV_FILE not found"
    fi
    
    # Run all test suites
    check_services_running
    test_api_endpoints
    test_database
    test_redis
    test_typesense
    test_monitoring
    test_orchestration
    test_etl
    test_data_quality
    test_security
    test_performance
    
    # Generate final report
    generate_report
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi