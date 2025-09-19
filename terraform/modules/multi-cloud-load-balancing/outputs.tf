# Outputs for Multi-Cloud Load Balancing Module

# ============================================================================
# AWS LOAD BALANCER OUTPUTS
# ============================================================================

output "aws_load_balancer_arn" {
  description = "ARN of the AWS Application Load Balancer"
  value       = var.deploy_to_aws ? aws_lb.main[0].arn : null
}

output "aws_load_balancer_dns_name" {
  description = "DNS name of the AWS Application Load Balancer"
  value       = var.deploy_to_aws ? aws_lb.main[0].dns_name : null
}

output "aws_load_balancer_zone_id" {
  description = "The canonical hosted zone ID of the AWS ALB"
  value       = var.deploy_to_aws ? aws_lb.main[0].zone_id : null
}

output "aws_target_group_arn" {
  description = "ARN of the AWS ALB target group"
  value       = var.deploy_to_aws ? aws_lb_target_group.api[0].arn : null
}

output "aws_target_group_arn_suffix" {
  description = "ARN suffix of the AWS ALB target group"
  value       = var.deploy_to_aws ? aws_lb_target_group.api[0].arn_suffix : null
}

output "aws_canary_target_group_arn" {
  description = "ARN of the AWS ALB canary target group"
  value       = var.deploy_to_aws && var.enable_canary_deployment ? aws_lb_target_group.api_canary[0].arn : null
}

output "aws_listener_arn" {
  description = "ARN of the AWS ALB HTTPS listener"
  value       = var.deploy_to_aws ? aws_lb_listener.api[0].arn : null
}

output "aws_security_group_id" {
  description = "Security group ID for the AWS ALB"
  value       = var.deploy_to_aws ? aws_security_group.alb[0].id : null
}

output "aws_access_logs_bucket" {
  description = "S3 bucket name for AWS ALB access logs"
  value       = var.deploy_to_aws && var.enable_access_logs ? aws_s3_bucket.alb_logs[0].id : null
}

output "aws_access_logs_bucket_arn" {
  description = "S3 bucket ARN for AWS ALB access logs"
  value       = var.deploy_to_aws && var.enable_access_logs ? aws_s3_bucket.alb_logs[0].arn : null
}

# ============================================================================
# AZURE LOAD BALANCER OUTPUTS
# ============================================================================

output "azure_application_gateway_id" {
  description = "ID of the Azure Application Gateway"
  value       = var.deploy_to_azure ? azurerm_application_gateway.main[0].id : null
}

output "azure_application_gateway_fqdn" {
  description = "FQDN of the Azure Application Gateway"
  value       = var.deploy_to_azure ? azurerm_public_ip.appgw[0].fqdn : null
}

output "azure_public_ip_address" {
  description = "Public IP address of the Azure Application Gateway"
  value       = var.deploy_to_azure ? azurerm_public_ip.appgw[0].ip_address : null
}

output "azure_public_ip_id" {
  description = "ID of the Azure public IP for Application Gateway"
  value       = var.deploy_to_azure ? azurerm_public_ip.appgw[0].id : null
}

output "azure_backend_address_pool_id" {
  description = "ID of the Azure Application Gateway backend address pool"
  value       = var.deploy_to_azure ? "${azurerm_application_gateway.main[0].id}/backendAddressPools/${local.project_name}-${local.environment}-backend-pool" : null
}

output "azure_application_gateway_name" {
  description = "Name of the Azure Application Gateway"
  value       = var.deploy_to_azure ? azurerm_application_gateway.main[0].name : null
}

# ============================================================================
# GCP LOAD BALANCER OUTPUTS
# ============================================================================

output "gcp_global_ip_address" {
  description = "Global static IP address for GCP load balancer"
  value       = var.deploy_to_gcp ? google_compute_global_address.main[0].address : null
}

output "gcp_global_ip_name" {
  description = "Name of the GCP global static IP"
  value       = var.deploy_to_gcp ? google_compute_global_address.main[0].name : null
}

output "gcp_forwarding_rule_id" {
  description = "ID of the GCP HTTPS forwarding rule"
  value       = var.deploy_to_gcp ? google_compute_global_forwarding_rule.https[0].id : null
}

output "gcp_url_map_id" {
  description = "ID of the GCP URL map"
  value       = var.deploy_to_gcp ? google_compute_url_map.main[0].id : null
}

output "gcp_backend_service_id" {
  description = "ID of the GCP backend service"
  value       = var.deploy_to_gcp ? google_compute_backend_service.api[0].id : null
}

output "gcp_ssl_certificate_id" {
  description = "ID of the GCP SSL certificate"
  value       = var.deploy_to_gcp ? google_compute_ssl_certificate.main[0].id : null
}

output "gcp_health_check_id" {
  description = "ID of the GCP health check"
  value       = var.deploy_to_gcp ? google_compute_health_check.api[0].id : null
}

output "gcp_security_policy_id" {
  description = "ID of the GCP Cloud Armor security policy"
  value       = var.deploy_to_gcp && var.enable_cloud_armor ? google_compute_security_policy.main[0].id : null
}

# ============================================================================
# GLOBAL LOAD BALANCING OUTPUTS
# ============================================================================

output "global_endpoints" {
  description = "Map of global endpoints for each cloud provider"
  value = {
    aws = var.deploy_to_aws ? {
      endpoint    = aws_lb.main[0].dns_name
      protocol    = "https"
      port        = 443
      health_path = local.health_check_config.path
      region      = "aws"
      priority    = 1
      weight      = local.traffic_weights.primary_region
    } : null
    
    azure = var.deploy_to_azure ? {
      endpoint    = azurerm_public_ip.appgw[0].fqdn
      protocol    = "https"
      port        = 443
      health_path = local.health_check_config.path
      region      = "azure"
      priority    = 2
      weight      = local.traffic_weights.secondary_region
    } : null
    
    gcp = var.deploy_to_gcp ? {
      endpoint    = google_compute_global_address.main[0].address
      protocol    = "https"
      port        = 443
      health_path = local.health_check_config.path
      region      = "gcp"
      priority    = 3
      weight      = local.traffic_weights.tertiary_region
    } : null
  }
}

output "primary_endpoint" {
  description = "Primary load balancer endpoint"
  value = var.deploy_to_aws ? "https://${aws_lb.main[0].dns_name}" : (
    var.deploy_to_azure ? "https://${azurerm_public_ip.appgw[0].fqdn}" : (
      var.deploy_to_gcp ? "https://${google_compute_global_address.main[0].address}" : null
    )
  )
}

output "secondary_endpoint" {
  description = "Secondary load balancer endpoint for failover"
  value = var.deploy_to_azure && var.deploy_to_aws ? "https://${azurerm_public_ip.appgw[0].fqdn}" : (
    var.deploy_to_gcp && (var.deploy_to_aws || var.deploy_to_azure) ? "https://${google_compute_global_address.main[0].address}" : null
  )
}

# ============================================================================
# HEALTH CHECK CONFIGURATION OUTPUTS
# ============================================================================

output "health_check_configuration" {
  description = "Health check configuration for monitoring"
  value = {
    path                = local.health_check_config.path
    port                = local.health_check_config.port
    protocol            = local.health_check_config.protocol
    interval_seconds    = local.health_check_config.interval_seconds
    timeout_seconds     = local.health_check_config.timeout_seconds
    healthy_threshold   = local.health_check_config.healthy_threshold
    unhealthy_threshold = local.health_check_config.unhealthy_threshold
    grace_period_seconds = local.health_check_config.grace_period_seconds
  }
}

# ============================================================================
# TRAFFIC MANAGEMENT OUTPUTS
# ============================================================================

output "traffic_distribution" {
  description = "Current traffic distribution across regions"
  value = {
    primary_region_weight   = local.traffic_weights.primary_region
    secondary_region_weight = local.traffic_weights.secondary_region
    tertiary_region_weight  = local.traffic_weights.tertiary_region
    total_weight           = local.traffic_weights.primary_region + local.traffic_weights.secondary_region + local.traffic_weights.tertiary_region
  }
}

output "failover_configuration" {
  description = "Failover configuration details"
  value = {
    automatic_failover_enabled = true
    failover_threshold_minutes = var.failover_threshold_minutes
    failback_delay_minutes     = var.failback_delay_minutes
    health_check_grace_period  = local.health_check_config.grace_period_seconds
    circuit_breaker_enabled    = true
    circuit_breaker_threshold  = var.circuit_breaker_failure_threshold
  }
}

# ============================================================================
# SECURITY OUTPUTS
# ============================================================================

output "security_features" {
  description = "Security features enabled across load balancers"
  value = {
    waf_enabled              = var.enable_waf
    ssl_termination_enabled  = true
    rate_limiting_enabled    = true
    geo_blocking_enabled     = length(var.blocked_countries) > 0
    ip_blocking_enabled      = length(var.blocked_ip_ranges) > 0
    ddos_protection_enabled  = var.deploy_to_gcp ? var.enable_cloud_armor : false
    
    rate_limit_config = {
      requests_per_minute = var.rate_limit_requests_per_minute
      burst_capacity     = var.rate_limit_requests_per_minute * 2
    }
    
    ssl_config = {
      min_tls_version     = "1.2"
      ssl_policy         = "modern"
      perfect_forward_secrecy = true
    }
  }
}

# ============================================================================
# MONITORING AND OBSERVABILITY OUTPUTS
# ============================================================================

output "monitoring_endpoints" {
  description = "Monitoring and metrics endpoints"
  value = {
    health_check_endpoints = compact([
      var.deploy_to_aws ? "https://${aws_lb.main[0].dns_name}${local.health_check_config.path}" : "",
      var.deploy_to_azure ? "https://${azurerm_public_ip.appgw[0].fqdn}${local.health_check_config.path}" : "",
      var.deploy_to_gcp ? "https://${google_compute_global_address.main[0].address}${local.health_check_config.path}" : ""
    ])
    
    metrics_endpoints = {
      aws_cloudwatch   = var.deploy_to_aws ? "cloudwatch:${local.project_name}-${local.environment}" : null
      azure_monitor    = var.deploy_to_azure ? "azuremonitor:${local.project_name}-${local.environment}" : null
      gcp_monitoring   = var.deploy_to_gcp ? "stackdriver:${local.project_name}-${local.environment}" : null
    }
    
    log_destinations = {
      aws_s3           = var.deploy_to_aws && var.enable_access_logs ? aws_s3_bucket.alb_logs[0].id : null
      azure_storage    = var.deploy_to_azure ? "${local.project_name}${local.environment}logs" : null
      gcp_cloud_logging = var.deploy_to_gcp ? "projects/${var.gcp_project_id}/logs/${local.project_name}-${local.environment}" : null
    }
  }
}

# ============================================================================
# PERFORMANCE METRICS OUTPUTS
# ============================================================================

output "performance_configuration" {
  description = "Performance optimization configuration"
  value = {
    connection_draining_timeout = var.connection_draining_timeout_seconds
    idle_timeout               = var.idle_timeout_seconds
    http2_enabled             = var.enable_http2
    compression_enabled       = var.enable_compression
    cdn_enabled               = var.enable_cdn
    
    latency_targets = {
      p50_target_ms = 100
      p90_target_ms = 500
      p95_target_ms = var.latency_threshold_ms
      p99_target_ms = var.latency_threshold_ms * 2
    }
    
    availability_targets = {
      sla_percentage    = var.availability_sla_percentage
      error_rate_threshold = var.error_rate_threshold
      uptime_target_minutes = (365 * 24 * 60) * (var.availability_sla_percentage / 100)
    }
  }
}

# ============================================================================
# COST OPTIMIZATION OUTPUTS
# ============================================================================

output "cost_optimization" {
  description = "Cost optimization features and targets"
  value = {
    optimization_enabled         = var.enable_cost_optimization
    target_reduction_percentage = var.cost_optimization_target_percentage
    
    cost_saving_features = {
      intelligent_routing        = var.enable_latency_routing
      regional_traffic_weighting = true
      auto_scaling_integration   = var.enable_auto_scaling_integration
      spot_instance_support     = false  # Not applicable for load balancers
      reserved_capacity_usage   = var.environment == "prod"
    }
    
    estimated_monthly_cost = {
      aws_alb    = var.deploy_to_aws ? "~$25-50" : "$0"
      azure_appgw = var.deploy_to_azure ? "~$40-80" : "$0"
      gcp_lb     = var.deploy_to_gcp ? "~$20-40" : "$0"
      data_transfer = "Variable based on traffic volume"
    }
  }
}

# ============================================================================
# DISASTER RECOVERY OUTPUTS
# ============================================================================

output "disaster_recovery" {
  description = "Disaster recovery configuration and capabilities"
  value = {
    multi_region_deployment = {
      aws_deployed    = var.deploy_to_aws
      azure_deployed  = var.deploy_to_azure
      gcp_deployed    = var.deploy_to_gcp
      total_regions   = sum([var.deploy_to_aws ? 1 : 0, var.deploy_to_azure ? 1 : 0, var.deploy_to_gcp ? 1 : 0])
    }
    
    recovery_objectives = {
      rto_minutes = var.recovery_time_objective_minutes
      rpo_minutes = var.recovery_point_objective_minutes
    }
    
    backup_configuration = {
      cross_region_backup_enabled = var.enable_cross_region_backup
      configuration_backup_enabled = true
      automated_failover_enabled  = true
      manual_failback_required    = false
    }
    
    failover_capabilities = {
      dns_based_failover    = var.enable_external_dns
      health_based_failover = true
      geographic_failover   = var.enable_geo_routing
      latency_based_failover = var.enable_latency_routing
    }
  }
}

# ============================================================================
# INTEGRATION OUTPUTS
# ============================================================================

output "integration_points" {
  description = "Integration points for other services"
  value = {
    kubernetes = {
      namespace           = var.kubernetes_namespace
      service_account    = var.service_account_name
      config_map_name    = kubernetes_config_map.global_health_check.metadata[0].name
      external_dns_enabled = var.enable_external_dns
    }
    
    dns = {
      global_domain_name = var.global_domain_name
      api_hostname      = var.api_hostname
      external_dns_enabled = var.enable_external_dns
    }
    
    monitoring = {
      prometheus_metrics_enabled = true
      health_check_monitoring    = true
      access_log_analysis       = var.enable_access_logs
      custom_metrics_enabled    = true
    }
    
    security = {
      waf_integration           = var.enable_waf
      certificate_management    = "automated"
      security_scanning_enabled = true
      compliance_logging       = var.enable_compliance_logging
    }
  }
}

# ============================================================================
# SUMMARY OUTPUT
# ============================================================================

output "load_balancer_summary" {
  description = "Comprehensive summary of load balancer deployment"
  value = {
    deployment_info = {
      project_name = local.project_name
      environment  = local.environment
      deployed_clouds = compact([
        var.deploy_to_aws ? "aws" : "",
        var.deploy_to_azure ? "azure" : "",
        var.deploy_to_gcp ? "gcp" : ""
      ])
      primary_endpoint = var.deploy_to_aws ? "https://${aws_lb.main[0].dns_name}" : (
        var.deploy_to_azure ? "https://${azurerm_public_ip.appgw[0].fqdn}" : (
          var.deploy_to_gcp ? "https://${google_compute_global_address.main[0].address}" : "none"
        )
      )
    }
    
    features_enabled = {
      multi_cloud           = sum([var.deploy_to_aws ? 1 : 0, var.deploy_to_azure ? 1 : 0, var.deploy_to_gcp ? 1 : 0]) > 1
      ssl_termination      = true
      health_checks        = true
      auto_scaling         = var.enable_auto_scaling_integration
      cdn                  = var.enable_cdn
      waf                  = var.enable_waf
      rate_limiting        = true
      geographic_routing   = var.enable_geo_routing
      canary_deployment    = var.enable_canary_deployment
      a_b_testing         = var.enable_a_b_testing
    }
    
    performance_targets = {
      availability_sla     = "${var.availability_sla_percentage}%"
      max_latency_p95     = "${var.latency_threshold_ms}ms"
      max_error_rate      = "${var.error_rate_threshold}%"
      cost_reduction      = "${var.cost_optimization_target_percentage}%"
    }
    
    security_compliance = {
      tls_version         = "1.2+"
      security_headers    = "enabled"
      rate_limiting      = "enabled"
      geo_blocking       = length(var.blocked_countries) > 0 ? "enabled" : "disabled"
      ddos_protection    = var.deploy_to_gcp && var.enable_cloud_armor ? "enabled" : "basic"
      compliance_logging = var.enable_compliance_logging ? "enabled" : "disabled"
    }
  }
}