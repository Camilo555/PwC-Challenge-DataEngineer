# Variables for Multi-Cloud Load Balancing Module

# ============================================================================
# PROJECT CONFIGURATION
# ============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "pwc-retail-platform"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "labels" {
  description = "Common labels to be applied to all resources"
  type        = map(string)
  default     = {}
}

# ============================================================================
# MULTI-CLOUD DEPLOYMENT FLAGS
# ============================================================================

variable "deploy_to_aws" {
  description = "Whether to deploy AWS load balancer resources"
  type        = bool
  default     = true
}

variable "deploy_to_azure" {
  description = "Whether to deploy Azure load balancer resources"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Whether to deploy GCP load balancer resources"
  type        = bool
  default     = false
}

# ============================================================================
# AWS CONFIGURATION
# ============================================================================

variable "aws_vpc_id" {
  description = "AWS VPC ID for load balancer deployment"
  type        = string
  default     = ""
}

variable "aws_vpc_cidr" {
  description = "AWS VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aws_public_subnet_ids" {
  description = "List of AWS public subnet IDs for ALB deployment"
  type        = list(string)
  default     = []
}

variable "aws_private_subnet_ids" {
  description = "List of AWS private subnet IDs for target groups"
  type        = list(string)
  default     = []
}

# ============================================================================
# AZURE CONFIGURATION
# ============================================================================

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_location" {
  description = "Azure location for resources"
  type        = string
  default     = "East US"
}

variable "azure_gateway_subnet_id" {
  description = "Azure subnet ID for Application Gateway"
  type        = string
  default     = ""
}

variable "azure_backend_fqdns" {
  description = "List of backend FQDNs for Azure Application Gateway"
  type        = list(string)
  default     = []
}

# ============================================================================
# GCP CONFIGURATION
# ============================================================================

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "gcp_instance_groups" {
  description = "List of GCP instance groups for backend service"
  type = list(object({
    instance_group = string
    zone          = string
  }))
  default = []
}

# ============================================================================
# SSL/TLS CONFIGURATION
# ============================================================================

variable "ssl_certificate_arn" {
  description = "AWS ACM certificate ARN for HTTPS listener"
  type        = string
  default     = ""
}

variable "ssl_certificate_data" {
  description = "SSL certificate data for Azure Application Gateway"
  type        = string
  default     = ""
  sensitive   = true
}

variable "ssl_certificate_password" {
  description = "SSL certificate password for Azure Application Gateway"
  type        = string
  default     = ""
  sensitive   = true
}

variable "ssl_certificate" {
  description = "SSL certificate for GCP load balancer"
  type        = string
  default     = ""
  sensitive   = true
}

variable "ssl_private_key" {
  description = "SSL private key for GCP load balancer"
  type        = string
  default     = ""
  sensitive   = true
}

# ============================================================================
# LOAD BALANCER FEATURES
# ============================================================================

variable "enable_sticky_sessions" {
  description = "Enable session affinity/sticky sessions"
  type        = bool
  default     = false
}

variable "enable_access_logs" {
  description = "Enable access logging for load balancers"
  type        = bool
  default     = true
}

variable "enable_health_check_logs" {
  description = "Enable health check logging"
  type        = bool
  default     = true
}

variable "enable_cdn" {
  description = "Enable CDN integration (GCP Cloud CDN)"
  type        = bool
  default     = true
}

variable "enable_cloud_armor" {
  description = "Enable GCP Cloud Armor security policies"
  type        = bool
  default     = true
}

variable "enable_waf" {
  description = "Enable Web Application Firewall"
  type        = bool
  default     = true
}

# ============================================================================
# ADVANCED ROUTING FEATURES
# ============================================================================

variable "enable_api_versioning" {
  description = "Enable API versioning routing rules"
  type        = bool
  default     = true
}

variable "enable_canary_deployment" {
  description = "Enable canary deployment routing"
  type        = bool
  default     = false
}

variable "enable_a_b_testing" {
  description = "Enable A/B testing routing rules"
  type        = bool
  default     = false
}

variable "enable_geo_routing" {
  description = "Enable geographic-based routing"
  type        = bool
  default     = true
}

variable "enable_latency_routing" {
  description = "Enable latency-based routing"
  type        = bool
  default     = true
}

variable "enable_weighted_routing" {
  description = "Enable weighted routing for traffic distribution"
  type        = bool
  default     = true
}

# ============================================================================
# COST OPTIMIZATION
# ============================================================================

variable "enable_cost_optimization" {
  description = "Enable cost optimization features"
  type        = bool
  default     = true
}

variable "cost_optimization_target_percentage" {
  description = "Target cost reduction percentage"
  type        = number
  default     = 40
  validation {
    condition     = var.cost_optimization_target_percentage >= 0 && var.cost_optimization_target_percentage <= 100
    error_message = "Cost optimization target must be between 0 and 100 percent."
  }
}

# ============================================================================
# SECURITY CONFIGURATION
# ============================================================================

variable "rate_limit_requests_per_minute" {
  description = "Rate limit requests per minute per IP"
  type        = number
  default     = 1000
}

variable "blocked_ip_ranges" {
  description = "List of IP ranges to block"
  type        = list(string)
  default     = []
}

variable "blocked_countries" {
  description = "List of country codes to block"
  type        = list(string)
  default     = []
}

variable "allowed_ip_ranges" {
  description = "List of allowed IP ranges (whitelist)"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ============================================================================
# HEALTH CHECK CONFIGURATION
# ============================================================================

variable "health_check_path" {
  description = "Health check endpoint path"
  type        = string
  default     = "/api/v1/health"
}

variable "health_check_interval_seconds" {
  description = "Health check interval in seconds"
  type        = number
  default     = 30
  validation {
    condition     = var.health_check_interval_seconds >= 5 && var.health_check_interval_seconds <= 300
    error_message = "Health check interval must be between 5 and 300 seconds."
  }
}

variable "health_check_timeout_seconds" {
  description = "Health check timeout in seconds"
  type        = number
  default     = 10
  validation {
    condition     = var.health_check_timeout_seconds >= 2 && var.health_check_timeout_seconds <= 60
    error_message = "Health check timeout must be between 2 and 60 seconds."
  }
}

variable "healthy_threshold" {
  description = "Number of consecutive successful health checks required"
  type        = number
  default     = 2
  validation {
    condition     = var.healthy_threshold >= 1 && var.healthy_threshold <= 10
    error_message = "Healthy threshold must be between 1 and 10."
  }
}

variable "unhealthy_threshold" {
  description = "Number of consecutive failed health checks required"
  type        = number
  default     = 3
  validation {
    condition     = var.unhealthy_threshold >= 1 && var.unhealthy_threshold <= 10
    error_message = "Unhealthy threshold must be between 1 and 10."
  }
}

# ============================================================================
# DOMAIN AND DNS CONFIGURATION
# ============================================================================

variable "global_domain_name" {
  description = "Global domain name for multi-cloud load balancing"
  type        = string
  default     = ""
}

variable "api_hostname" {
  description = "Hostname for API endpoints"
  type        = string
  default     = ""
}

variable "enable_external_dns" {
  description = "Enable external DNS management"
  type        = bool
  default     = true
}

# ============================================================================
# KUBERNETES INTEGRATION
# ============================================================================

variable "kubernetes_namespace" {
  description = "Kubernetes namespace for load balancer resources"
  type        = string
  default     = "default"
}

variable "service_account_name" {
  description = "Kubernetes service account name"
  type        = string
  default     = "load-balancer-controller"
}

# ============================================================================
# MONITORING AND ALERTING
# ============================================================================

variable "enable_monitoring" {
  description = "Enable comprehensive monitoring"
  type        = bool
  default     = true
}

variable "enable_alerting" {
  description = "Enable alerting for load balancer issues"
  type        = bool
  default     = true
}

variable "latency_threshold_ms" {
  description = "Latency threshold in milliseconds for alerts"
  type        = number
  default     = 1000
}

variable "error_rate_threshold" {
  description = "Error rate threshold percentage for alerts"
  type        = number
  default     = 5.0
  validation {
    condition     = var.error_rate_threshold >= 0 && var.error_rate_threshold <= 100
    error_message = "Error rate threshold must be between 0 and 100 percent."
  }
}

variable "availability_sla_percentage" {
  description = "Availability SLA percentage"
  type        = number
  default     = 99.99
  validation {
    condition     = var.availability_sla_percentage >= 90 && var.availability_sla_percentage <= 100
    error_message = "Availability SLA must be between 90 and 100 percent."
  }
}

variable "alert_notification_channels" {
  description = "List of notification channels for alerts"
  type        = list(string)
  default     = []
}

# ============================================================================
# TRAFFIC MANAGEMENT
# ============================================================================

variable "primary_region_weight" {
  description = "Traffic weight for primary region (0-100)"
  type        = number
  default     = 70
  validation {
    condition     = var.primary_region_weight >= 0 && var.primary_region_weight <= 100
    error_message = "Primary region weight must be between 0 and 100."
  }
}

variable "secondary_region_weight" {
  description = "Traffic weight for secondary region (0-100)"
  type        = number
  default     = 25
  validation {
    condition     = var.secondary_region_weight >= 0 && var.secondary_region_weight <= 100
    error_message = "Secondary region weight must be between 0 and 100."
  }
}

variable "tertiary_region_weight" {
  description = "Traffic weight for tertiary region (0-100)"
  type        = number
  default     = 5
  validation {
    condition     = var.tertiary_region_weight >= 0 && var.tertiary_region_weight <= 100
    error_message = "Tertiary region weight must be between 0 and 100."
  }
}

variable "failover_threshold_minutes" {
  description = "Minutes of consecutive failures before triggering failover"
  type        = number
  default     = 5
  validation {
    condition     = var.failover_threshold_minutes >= 1 && var.failover_threshold_minutes <= 60
    error_message = "Failover threshold must be between 1 and 60 minutes."
  }
}

variable "failback_delay_minutes" {
  description = "Minutes to wait before failing back to primary region"
  type        = number
  default     = 10
  validation {
    condition     = var.failback_delay_minutes >= 1 && var.failback_delay_minutes <= 120
    error_message = "Failback delay must be between 1 and 120 minutes."
  }
}

# ============================================================================
# CANARY AND A/B TESTING
# ============================================================================

variable "canary_weight_percentage" {
  description = "Percentage of traffic to route to canary deployment"
  type        = number
  default     = 10
  validation {
    condition     = var.canary_weight_percentage >= 0 && var.canary_weight_percentage <= 50
    error_message = "Canary weight must be between 0 and 50 percent."
  }
}

variable "canary_cookie_duration_seconds" {
  description = "Duration for canary deployment sticky sessions"
  type        = number
  default     = 3600
}

variable "a_b_test_variants" {
  description = "A/B test variant configurations"
  type = map(object({
    weight_percentage = number
    header_value     = string
    cookie_name      = string
  }))
  default = {}
}

# ============================================================================
# PERFORMANCE OPTIMIZATION
# ============================================================================

variable "connection_draining_timeout_seconds" {
  description = "Connection draining timeout in seconds"
  type        = number
  default     = 300
  validation {
    condition     = var.connection_draining_timeout_seconds >= 0 && var.connection_draining_timeout_seconds <= 3600
    error_message = "Connection draining timeout must be between 0 and 3600 seconds."
  }
}

variable "idle_timeout_seconds" {
  description = "Idle connection timeout in seconds"
  type        = number
  default     = 60
  validation {
    condition     = var.idle_timeout_seconds >= 1 && var.idle_timeout_seconds <= 4000
    error_message = "Idle timeout must be between 1 and 4000 seconds."
  }
}

variable "enable_http2" {
  description = "Enable HTTP/2 support"
  type        = bool
  default     = true
}

variable "enable_compression" {
  description = "Enable response compression"
  type        = bool
  default     = true
}

# ============================================================================
# LOGGING AND COMPLIANCE
# ============================================================================

variable "log_retention_days" {
  description = "Number of days to retain access logs"
  type        = number
  default     = 90
  validation {
    condition     = var.log_retention_days >= 1 && var.log_retention_days <= 3653
    error_message = "Log retention must be between 1 and 3653 days."
  }
}

variable "enable_request_tracing" {
  description = "Enable distributed request tracing"
  type        = bool
  default     = true
}

variable "enable_compliance_logging" {
  description = "Enable compliance-focused logging"
  type        = bool
  default     = true
}

variable "log_sampling_rate" {
  description = "Sampling rate for access logs (0.0 to 1.0)"
  type        = number
  default     = 1.0
  validation {
    condition     = var.log_sampling_rate >= 0.0 && var.log_sampling_rate <= 1.0
    error_message = "Log sampling rate must be between 0.0 and 1.0."
  }
}

# ============================================================================
# CIRCUIT BREAKER AND RESILIENCE
# ============================================================================

variable "circuit_breaker_failure_threshold" {
  description = "Number of failures before opening circuit breaker"
  type        = number
  default     = 5
  validation {
    condition     = var.circuit_breaker_failure_threshold >= 1 && var.circuit_breaker_failure_threshold <= 100
    error_message = "Circuit breaker failure threshold must be between 1 and 100."
  }
}

variable "circuit_breaker_recovery_timeout_seconds" {
  description = "Seconds before attempting circuit breaker recovery"
  type        = number
  default     = 60
  validation {
    condition     = var.circuit_breaker_recovery_timeout_seconds >= 1 && var.circuit_breaker_recovery_timeout_seconds <= 3600
    error_message = "Circuit breaker recovery timeout must be between 1 and 3600 seconds."
  }
}

variable "circuit_breaker_half_open_max_requests" {
  description = "Maximum requests allowed in half-open state"
  type        = number
  default     = 10
  validation {
    condition     = var.circuit_breaker_half_open_max_requests >= 1 && var.circuit_breaker_half_open_max_requests <= 100
    error_message = "Half-open max requests must be between 1 and 100."
  }
}

# ============================================================================
# DISASTER RECOVERY
# ============================================================================

variable "enable_cross_region_backup" {
  description = "Enable cross-region backup for load balancer configuration"
  type        = bool
  default     = true
}

variable "recovery_time_objective_minutes" {
  description = "Recovery Time Objective (RTO) in minutes"
  type        = number
  default     = 60
  validation {
    condition     = var.recovery_time_objective_minutes >= 1 && var.recovery_time_objective_minutes <= 1440
    error_message = "RTO must be between 1 and 1440 minutes (24 hours)."
  }
}

variable "recovery_point_objective_minutes" {
  description = "Recovery Point Objective (RPO) in minutes"
  type        = number
  default     = 15
  validation {
    condition     = var.recovery_point_objective_minutes >= 1 && var.recovery_point_objective_minutes <= 1440
    error_message = "RPO must be between 1 and 1440 minutes (24 hours)."
  }
}

# ============================================================================
# AUTO-SCALING INTEGRATION
# ============================================================================

variable "enable_auto_scaling_integration" {
  description = "Enable integration with auto-scaling groups"
  type        = bool
  default     = true
}

variable "auto_scaling_target_utilization" {
  description = "Target utilization percentage for auto-scaling"
  type        = number
  default     = 70
  validation {
    condition     = var.auto_scaling_target_utilization >= 10 && var.auto_scaling_target_utilization <= 90
    error_message = "Auto-scaling target utilization must be between 10 and 90 percent."
  }
}

variable "auto_scaling_scale_up_cooldown_seconds" {
  description = "Cooldown period for scale-up operations"
  type        = number
  default     = 300
  validation {
    condition     = var.auto_scaling_scale_up_cooldown_seconds >= 60 && var.auto_scaling_scale_up_cooldown_seconds <= 3600
    error_message = "Scale-up cooldown must be between 60 and 3600 seconds."
  }
}

variable "auto_scaling_scale_down_cooldown_seconds" {
  description = "Cooldown period for scale-down operations"
  type        = number
  default     = 900
  validation {
    condition     = var.auto_scaling_scale_down_cooldown_seconds >= 60 && var.auto_scaling_scale_down_cooldown_seconds <= 3600
    error_message = "Scale-down cooldown must be between 60 and 3600 seconds."
  }
}