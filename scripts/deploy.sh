#!/bin/bash

# =============================================================================
# DEPLOYMENT SCRIPT FOR PWC RETAIL DATA PLATFORM
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENVIRONMENTS=("test" "dev" "prod")
DEFAULT_PROFILES=("api" "etl" "orchestration")

# Functions
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

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] ENVIRONMENT

Deploy PWC Retail Data Platform to specified environment.

ENVIRONMENTS:
  test        Deploy to test environment (lightweight)
  dev         Deploy to development environment (with dev tools)
  prod        Deploy to production environment (full stack)

OPTIONS:
  -p, --profiles PROFILES   Comma-separated list of profiles to deploy
                           (default: api,etl,orchestration)
  -d, --detach             Run in detached mode
  -b, --build              Force rebuild of images
  -v, --verbose            Verbose output
  -h, --help               Show this help message
  --down                   Stop and remove containers
  --logs                   Show logs for running services
  --status                 Show status of services

EXAMPLES:
  $0 dev                                    # Deploy dev environment
  $0 prod --profiles api,monitoring         # Deploy prod with API and monitoring
  $0 test --build --detach                  # Build and deploy test environment
  $0 dev --down                             # Stop dev environment
  $0 prod --logs                            # Show prod logs

PROFILES:
  api              API services
  etl              ETL processing services  
  orchestration    Dagster orchestration
  dagster          Dagster only
  airflow          Airflow only
  monitoring       Monitoring stack (Datadog, Prometheus, Grafana)
  dev-monitoring   Development monitoring
  test-monitoring  Test monitoring  
  dev-tools        Development tools (PgAdmin, Jupyter)
  dev-airflow      Development Airflow
  kafka-cluster    Multi-broker Kafka cluster (prod only)
  backup           Backup services (prod only)
  ci               CI/CD services (test only)
  integration-test Integration testing (test only)

EOF
}

validate_environment() {
    local env=$1
    if [[ ! " ${ENVIRONMENTS[@]} " =~ " ${env} " ]]; then
        log_error "Invalid environment: $env"
        log_info "Valid environments: ${ENVIRONMENTS[*]}"
        exit 1
    fi
}

check_prerequisites() {
    local env=$1
    
    log_info "Checking prerequisites for $env environment..."
    
    # Check Docker and Docker Compose
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is not installed"
        exit 1
    fi
    
    # Check environment file
    local env_file=".env.$env"
    if [[ ! -f "$PROJECT_DIR/$env_file" ]]; then
        log_error "Environment file $env_file not found"
        exit 1
    fi
    
    # Check required directories
    local data_dir="$PROJECT_DIR/data/$env"
    local logs_dir="$PROJECT_DIR/logs/$env"
    
    mkdir -p "$data_dir" "$logs_dir"
    
    # Environment-specific checks
    case $env in
        prod)
            # Check production-specific requirements
            if [[ ! -f "$PROJECT_DIR/ssl/server.crt" ]]; then
                log_warning "SSL certificate not found - HTTPS will not work"
            fi
            
            # Check for secrets
            if grep -q "CHANGE_ME" "$PROJECT_DIR/$env_file" 2>/dev/null; then
                log_error "Production secrets not configured in $env_file"
                exit 1
            fi
            ;;
        dev)
            # Check development tools
            if [[ ! -d "$PROJECT_DIR/notebooks" ]]; then
                mkdir -p "$PROJECT_DIR/notebooks"
                log_info "Created notebooks directory for Jupyter"
            fi
            ;;
        test)
            # Check test data
            if [[ ! -d "$PROJECT_DIR/data/test" ]]; then
                mkdir -p "$PROJECT_DIR/data/test"
                log_info "Created test data directory"
            fi
            ;;
    esac
    
    log_success "Prerequisites check completed"
}

deploy_environment() {
    local env=$1
    local profiles=$2
    local compose_args=("${@:3}")
    
    log_info "Deploying $env environment with profiles: $profiles"
    
    cd "$PROJECT_DIR"
    
    # Set environment file
    export COMPOSE_FILE="docker-compose.base.yml:docker-compose.$env.yml"
    export COMPOSE_PROFILES="$profiles"
    
    # Load environment variables
    set -a
    source ".env.$env"
    set +a
    
    # Build compose command
    local compose_cmd=(
        "docker-compose"
        "-f" "docker-compose.base.yml"
        "-f" "docker-compose.$env.yml"
        "--env-file" ".env.$env"
    )
    
    # Add profiles
    if [[ -n "$profiles" ]]; then
        IFS=',' read -ra PROFILE_ARRAY <<< "$profiles"
        for profile in "${PROFILE_ARRAY[@]}"; do
            compose_cmd+=("--profile" "$profile")
        done
    fi
    
    # Execute command
    log_info "Running: ${compose_cmd[*]} up ${compose_args[*]}"
    "${compose_cmd[@]}" up "${compose_args[@]}"
}

stop_environment() {
    local env=$1
    
    log_info "Stopping $env environment..."
    
    cd "$PROJECT_DIR"
    
    docker-compose \
        -f docker-compose.base.yml \
        -f "docker-compose.$env.yml" \
        --env-file ".env.$env" \
        down --remove-orphans
    
    log_success "$env environment stopped"
}

show_logs() {
    local env=$1
    local services=("${@:2}")
    
    cd "$PROJECT_DIR"
    
    local compose_cmd=(
        "docker-compose"
        "-f" "docker-compose.base.yml"
        "-f" "docker-compose.$env.yml"
        "--env-file" ".env.$env"
        "logs"
        "-f"
    )
    
    if [[ ${#services[@]} -gt 0 ]]; then
        compose_cmd+=("${services[@]}")
    fi
    
    "${compose_cmd[@]}"
}

show_status() {
    local env=$1
    
    cd "$PROJECT_DIR"
    
    log_info "Status of $env environment:"
    
    docker-compose \
        -f docker-compose.base.yml \
        -f "docker-compose.$env.yml" \
        --env-file ".env.$env" \
        ps
}

# Main script
main() {
    local environment=""
    local profiles=""
    local detach=""
    local build=""
    local verbose=""
    local action="deploy"
    local services=()
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -p|--profiles)
                profiles="$2"
                shift 2
                ;;
            -d|--detach)
                detach="-d"
                shift
                ;;
            -b|--build)
                build="--build"
                shift
                ;;
            -v|--verbose)
                verbose="--verbose"
                set -x
                shift
                ;;
            --down)
                action="down"
                shift
                ;;
            --logs)
                action="logs"
                shift
                ;;
            --status)
                action="status"
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                if [[ -z "$environment" ]]; then
                    environment="$1"
                else
                    services+=("$1")
                fi
                shift
                ;;
        esac
    done
    
    # Validate environment
    if [[ -z "$environment" ]]; then
        log_error "Environment is required"
        usage
        exit 1
    fi
    
    validate_environment "$environment"
    
    # Set default profiles
    if [[ -z "$profiles" ]]; then
        profiles=$(IFS=,; echo "${DEFAULT_PROFILES[*]}")
    fi
    
    # Execute action
    case $action in
        deploy)
            check_prerequisites "$environment"
            
            # Build compose arguments
            local compose_args=()
            [[ -n "$detach" ]] && compose_args+=("$detach")
            [[ -n "$build" ]] && compose_args+=("$build")
            [[ -n "$verbose" ]] && compose_args+=("$verbose")
            
            deploy_environment "$environment" "$profiles" "${compose_args[@]}"
            
            log_success "$environment environment deployed successfully!"
            ;;
        down)
            stop_environment "$environment"
            ;;
        logs)
            show_logs "$environment" "${services[@]}"
            ;;
        status)
            show_status "$environment"
            ;;
    esac
}

# Run main function
main "$@"