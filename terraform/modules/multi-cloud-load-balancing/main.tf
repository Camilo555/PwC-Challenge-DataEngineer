# Multi-Cloud Advanced Load Balancing with Health-Aware Routing
# Production-ready load balancing across AWS, Azure, and GCP with intelligent traffic management

terraform {
  required_version = ">= 1.6.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
}

# ============================================================================
# LOCAL VALUES FOR LOAD BALANCER CONFIGURATION
# ============================================================================

locals {
  project_name = var.project_name
  environment  = var.environment
  
  # Health check configuration
  health_check_config = {
    path                = "/api/v1/health"
    port                = 80
    protocol            = "HTTP"
    interval_seconds    = 30
    timeout_seconds     = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
    grace_period_seconds = 300
  }
  
  # Traffic distribution weights based on region and cost
  traffic_weights = var.enable_cost_optimization ? {
    primary_region   = 70  # Primary region gets most traffic
    secondary_region = 25  # Secondary for failover
    tertiary_region  = 5   # Minimal traffic for warm standby
  } : {
    primary_region   = 60  # More even distribution for performance
    secondary_region = 30
    tertiary_region  = 10
  }
  
  # Common labels for all load balancer resources
  common_labels = merge(var.labels, {
    component = "load-balancer"
    tier      = "multi-cloud"
    managed_by = "terraform"
  })
}

# ============================================================================
# AWS APPLICATION LOAD BALANCER WITH ADVANCED FEATURES
# ============================================================================

# AWS ALB with intelligent routing
resource "aws_lb" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name               = "${local.project_name}-${local.environment}-alb"
  load_balancer_type = "application"
  scheme            = "internet-facing"
  
  # Multi-AZ deployment for high availability
  subnets = var.aws_public_subnet_ids
  
  security_groups = [aws_security_group.alb[0].id]
  
  # Advanced ALB features
  enable_deletion_protection     = var.environment == "prod" ? true : false
  enable_cross_zone_load_balancing = true
  enable_http2                   = true
  enable_waf_fail_open          = false
  
  # Access logs for monitoring and compliance
  access_logs {
    bucket  = aws_s3_bucket.alb_logs[0].id
    prefix  = "alb-logs"
    enabled = var.enable_access_logs
  }
  
  # Connection draining
  idle_timeout                     = 60
  enable_deletion_protection       = var.environment == "prod"
  drop_invalid_header_fields      = true
  preserve_host_header            = true
  desync_mitigation_mode          = "defensive"
  
  tags = merge(local.common_labels, {
    Name        = "${local.project_name}-${local.environment}-alb"
    Cloud       = "aws"
    Environment = local.environment
  })
}

# ALB Target Group with advanced health checks
resource "aws_lb_target_group" "api" {
  count = var.deploy_to_aws ? 1 : 0
  
  name     = "${local.project_name}-${local.environment}-api-tg"
  port     = 80
  protocol = "HTTP"
  vpc_id   = var.aws_vpc_id
  
  # Advanced health check configuration
  health_check {
    enabled             = true
    healthy_threshold   = local.health_check_config.healthy_threshold
    unhealthy_threshold = local.health_check_config.unhealthy_threshold
    timeout             = local.health_check_config.timeout_seconds
    interval            = local.health_check_config.interval_seconds
    path                = local.health_check_config.path
    port                = "traffic-port"
    protocol            = local.health_check_config.protocol
    matcher             = "200-299"
    
    # Custom health check headers
    # Uncomment if needed
    # grace_period_seconds = local.health_check_config.grace_period_seconds
  }
  
  # Connection draining
  deregistration_delay = var.environment == "prod" ? 300 : 60
  
  # Sticky sessions for stateful applications
  stickiness {
    type            = "lb_cookie"
    cookie_duration = 86400  # 24 hours
    enabled         = var.enable_sticky_sessions
  }
  
  # Target group attributes for performance optimization
  target_type = "ip"
  
  # Advanced routing attributes
  slow_start                = var.environment == "prod" ? 60 : 0
  load_balancing_algorithm_type = "least_outstanding_requests"
  
  tags = merge(local.common_labels, {
    Name = "${local.project_name}-${local.environment}-api-tg"
    Cloud = "aws"
  })
}

# ALB Listener with advanced routing rules
resource "aws_lb_listener" "api" {
  count = var.deploy_to_aws ? 1 : 0
  
  load_balancer_arn = aws_lb.main[0].arn
  port              = "443"
  protocol          = "HTTPS"
  ssl_policy        = "ELBSecurityPolicy-TLS-1-2-2017-01"
  certificate_arn   = var.ssl_certificate_arn
  
  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api[0].arn
  }
  
  tags = local.common_labels
}

# Advanced routing rules for API versioning and A/B testing
resource "aws_lb_listener_rule" "api_versioning" {
  count = var.deploy_to_aws && var.enable_api_versioning ? 1 : 0
  
  listener_arn = aws_lb_listener.api[0].arn
  priority     = 100
  
  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.api[0].arn
  }
  
  condition {
    path_pattern {
      values = ["/api/v2/*"]
    }
  }
  
  condition {
    http_header {
      http_header_name = "X-API-Version"
      values          = ["2.0"]
    }
  }
  
  tags = local.common_labels
}

# Weighted routing for canary deployments
resource "aws_lb_listener_rule" "canary_routing" {
  count = var.deploy_to_aws && var.enable_canary_deployment ? 1 : 0
  
  listener_arn = aws_lb_listener.api[0].arn
  priority     = 200
  
  action {
    type = "forward"
    forward {
      target_group {
        arn    = aws_lb_target_group.api[0].arn
        weight = 90  # 90% to stable version
      }
      
      target_group {
        arn    = aws_lb_target_group.api_canary[0].arn
        weight = 10  # 10% to canary version
      }
      
      stickiness {
        enabled  = true
        duration = 3600  # 1 hour
      }
    }
  }
  
  condition {
    http_header {
      http_header_name = "X-Canary-Enabled"
      values          = ["true"]
    }
  }
  
  tags = local.common_labels
}

# Canary target group
resource "aws_lb_target_group" "api_canary" {
  count = var.deploy_to_aws && var.enable_canary_deployment ? 1 : 0
  
  name     = "${local.project_name}-${local.environment}-api-canary-tg"
  port     = 80
  protocol = "HTTP"
  vpc_id   = var.aws_vpc_id
  
  health_check {
    enabled             = true
    healthy_threshold   = local.health_check_config.healthy_threshold
    unhealthy_threshold = local.health_check_config.unhealthy_threshold
    timeout             = local.health_check_config.timeout_seconds
    interval            = local.health_check_config.interval_seconds
    path                = local.health_check_config.path
    port                = "traffic-port"
    protocol            = local.health_check_config.protocol
    matcher             = "200-299"
  }
  
  deregistration_delay = 60  # Faster draining for canary
  
  tags = merge(local.common_labels, {
    Name = "${local.project_name}-${local.environment}-api-canary-tg"
    Cloud = "aws"
    Deployment = "canary"
  })
}

# ALB Security Group with restrictive rules
resource "aws_security_group" "alb" {
  count = var.deploy_to_aws ? 1 : 0
  
  name_prefix = "${local.project_name}-${local.environment}-alb-"
  vpc_id      = var.aws_vpc_id
  description = "Security group for Application Load Balancer"
  
  # HTTP from anywhere (redirect to HTTPS)
  ingress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTP from internet"
  }
  
  # HTTPS from anywhere
  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "HTTPS from internet"
  }
  
  # Health checks from ALB to targets
  egress {
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
    description = "HTTP to application targets"
  }
  
  # HTTPS to application targets
  egress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.aws_vpc_cidr]
    description = "HTTPS to application targets"
  }
  
  tags = merge(local.common_labels, {
    Name = "${local.project_name}-${local.environment}-alb-sg"
    Cloud = "aws"
  })
  
  lifecycle {
    create_before_destroy = true
  }
}

# S3 bucket for ALB access logs
resource "aws_s3_bucket" "alb_logs" {
  count = var.deploy_to_aws && var.enable_access_logs ? 1 : 0
  
  bucket        = "${local.project_name}-${local.environment}-alb-logs-${random_string.bucket_suffix.result}"
  force_destroy = var.environment != "prod"
  
  tags = merge(local.common_labels, {
    Name = "${local.project_name}-${local.environment}-alb-logs"
    Cloud = "aws"
  })
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "alb_logs" {
  count = var.deploy_to_aws && var.enable_access_logs ? 1 : 0
  
  bucket = aws_s3_bucket.alb_logs[0].id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "alb_logs" {
  count = var.deploy_to_aws && var.enable_access_logs ? 1 : 0
  
  bucket = aws_s3_bucket.alb_logs[0].id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# S3 bucket policy for ALB access logs
resource "aws_s3_bucket_policy" "alb_logs" {
  count = var.deploy_to_aws && var.enable_access_logs ? 1 : 0
  
  bucket = aws_s3_bucket.alb_logs[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = data.aws_elb_service_account.main[0].arn
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.alb_logs[0].arn}/*"
      },
      {
        Effect = "Allow"
        Principal = {
          Service = "delivery.logs.amazonaws.com"
        }
        Action   = "s3:PutObject"
        Resource = "${aws_s3_bucket.alb_logs[0].arn}/*"
        Condition = {
          StringEquals = {
            "s3:x-amz-acl" = "bucket-owner-full-control"
          }
        }
      }
    ]
  })
}

# Data source for ELB service account
data "aws_elb_service_account" "main" {
  count = var.deploy_to_aws && var.enable_access_logs ? 1 : 0
}

# Random string for unique bucket naming
resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# ============================================================================
# AZURE APPLICATION GATEWAY WITH WAF
# ============================================================================

# Azure Application Gateway with WAF v2
resource "azurerm_application_gateway" "main" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.project_name}-${local.environment}-appgw"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  
  sku {
    name     = var.environment == "prod" ? "WAF_v2" : "Standard_v2"
    tier     = var.environment == "prod" ? "WAF_v2" : "Standard_v2"
    capacity = var.environment == "prod" ? 3 : 2
  }
  
  # Auto-scaling configuration
  autoscale_configuration {
    min_capacity = var.environment == "prod" ? 2 : 1
    max_capacity = var.environment == "prod" ? 10 : 5
  }
  
  gateway_ip_configuration {
    name      = "gateway-ip-configuration"
    subnet_id = var.azure_gateway_subnet_id
  }
  
  # Frontend IP configuration
  frontend_ip_configuration {
    name                 = "frontend-ip"
    public_ip_address_id = azurerm_public_ip.appgw[0].id
  }
  
  # Frontend port configurations
  frontend_port {
    name = "http-port"
    port = 80
  }
  
  frontend_port {
    name = "https-port"
    port = 443
  }
  
  # Backend address pool
  backend_address_pool {
    name = "${local.project_name}-${local.environment}-backend-pool"
    fqdns = var.azure_backend_fqdns
  }
  
  # Backend HTTP settings with advanced health checks
  backend_http_settings {
    name                  = "backend-http-settings"
    cookie_based_affinity = var.enable_sticky_sessions ? "Enabled" : "Disabled"
    port                  = 80
    protocol              = "Http"
    request_timeout       = 60
    connection_draining {
      enabled           = true
      drain_timeout_sec = var.environment == "prod" ? 300 : 60
    }
    
    # Custom health probe
    probe_name = "api-health-probe"
  }
  
  # HTTPS backend settings
  backend_http_settings {
    name                  = "backend-https-settings"
    cookie_based_affinity = var.enable_sticky_sessions ? "Enabled" : "Disabled"
    port                  = 443
    protocol              = "Https"
    request_timeout       = 60
    
    probe_name = "api-health-probe-https"
  }
  
  # Custom health probes
  probe {
    name                                      = "api-health-probe"
    protocol                                  = "Http"
    path                                      = local.health_check_config.path
    host                                      = var.api_hostname
    interval                                  = local.health_check_config.interval_seconds
    timeout                                   = local.health_check_config.timeout_seconds
    unhealthy_threshold                       = local.health_check_config.unhealthy_threshold
    minimum_servers                          = 0
    pick_host_name_from_backend_http_settings = false
    
    match {
      status_code = ["200-399"]
    }
  }
  
  probe {
    name                                      = "api-health-probe-https"
    protocol                                  = "Https"
    path                                      = local.health_check_config.path
    host                                      = var.api_hostname
    interval                                  = local.health_check_config.interval_seconds
    timeout                                   = local.health_check_config.timeout_seconds
    unhealthy_threshold                       = local.health_check_config.unhealthy_threshold
    minimum_servers                          = 0
    pick_host_name_from_backend_http_settings = false
    
    match {
      status_code = ["200-399"]
    }
  }
  
  # HTTP listener (redirect to HTTPS)
  http_listener {
    name                           = "http-listener"
    frontend_ip_configuration_name = "frontend-ip"
    frontend_port_name             = "http-port"
    protocol                       = "Http"
  }
  
  # HTTPS listener
  http_listener {
    name                           = "https-listener"
    frontend_ip_configuration_name = "frontend-ip"
    frontend_port_name             = "https-port"
    protocol                       = "Https"
    ssl_certificate_name           = "ssl-certificate"
    require_sni                    = true
  }
  
  # SSL certificate
  ssl_certificate {
    name     = "ssl-certificate"
    data     = var.ssl_certificate_data
    password = var.ssl_certificate_password
  }
  
  # Redirect HTTP to HTTPS
  redirect_configuration {
    name                 = "http-to-https-redirect"
    redirect_type        = "Permanent"
    target_listener_name = "https-listener"
    include_path         = true
    include_query_string = true
  }
  
  # HTTP request routing rule (redirect)
  request_routing_rule {
    name                        = "http-redirect-rule"
    rule_type                   = "Basic"
    priority                    = 1000
    http_listener_name          = "http-listener"
    redirect_configuration_name = "http-to-https-redirect"
  }
  
  # HTTPS request routing rule
  request_routing_rule {
    name                       = "https-routing-rule"
    rule_type                  = "Basic"
    priority                   = 2000
    http_listener_name         = "https-listener"
    backend_address_pool_name  = "${local.project_name}-${local.environment}-backend-pool"
    backend_http_settings_name = "backend-https-settings"
  }
  
  # WAF configuration for production
  dynamic "waf_configuration" {
    for_each = var.environment == "prod" ? [1] : []
    content {
      enabled                  = true
      firewall_mode           = "Prevention"
      rule_set_type           = "OWASP"
      rule_set_version        = "3.2"
      file_upload_limit_mb    = 100
      request_body_check      = true
      max_request_body_size_kb = 128
      
      # Custom rules for API protection
      disabled_rule_group {
        rule_group_name = "REQUEST-942-APPLICATION-ATTACK-SQLI"
        rules          = [942100, 942110]
      }
      
      # Exclusions for false positives
      exclusion {
        match_variable          = "RequestHeaderNames"
        selector_match_operator = "Equals"
        selector               = "User-Agent"
      }
    }
  }
  
  tags = merge(local.common_labels, {
    Name  = "${local.project_name}-${local.environment}-appgw"
    Cloud = "azure"
  })
  
  depends_on = [azurerm_public_ip.appgw]
}

# Azure Public IP for Application Gateway
resource "azurerm_public_ip" "appgw" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.project_name}-${local.environment}-appgw-pip"
  location           = var.azure_location
  resource_group_name = var.azure_resource_group_name
  allocation_method   = "Static"
  sku                = "Standard"
  
  # DNS settings
  domain_name_label = "${local.project_name}-${local.environment}-api"
  
  tags = merge(local.common_labels, {
    Name  = "${local.project_name}-${local.environment}-appgw-pip"
    Cloud = "azure"
  })
}

# ============================================================================
# GOOGLE CLOUD LOAD BALANCER WITH CLOUD CDN
# ============================================================================

# GCP Global Load Balancer with Cloud CDN
resource "google_compute_global_forwarding_rule" "https" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name       = "${local.project_name}-${local.environment}-https-forwarding-rule"
  target     = google_compute_target_https_proxy.main[0].id
  port_range = "443"
  ip_address = google_compute_global_address.main[0].address
  
  labels = local.common_labels
}

# HTTP forwarding rule (redirect to HTTPS)
resource "google_compute_global_forwarding_rule" "http" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name       = "${local.project_name}-${local.environment}-http-forwarding-rule"
  target     = google_compute_target_http_proxy.redirect[0].id
  port_range = "80"
  ip_address = google_compute_global_address.main[0].address
  
  labels = local.common_labels
}

# Global static IP address
resource "google_compute_global_address" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name         = "${local.project_name}-${local.environment}-global-ip"
  description  = "Global static IP for load balancer"
  ip_version   = "IPV4"
  address_type = "EXTERNAL"
  
  labels = local.common_labels
}

# HTTPS target proxy
resource "google_compute_target_https_proxy" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name             = "${local.project_name}-${local.environment}-https-proxy"
  url_map          = google_compute_url_map.main[0].id
  ssl_certificates = [google_compute_ssl_certificate.main[0].id]
  
  # Security policies
  ssl_policy = google_compute_ssl_policy.modern[0].id
  
  description = "HTTPS target proxy for ${local.project_name}"
}

# HTTP target proxy for redirects
resource "google_compute_target_http_proxy" "redirect" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name    = "${local.project_name}-${local.environment}-http-redirect-proxy"
  url_map = google_compute_url_map.redirect[0].id
  
  description = "HTTP redirect proxy for ${local.project_name}"
}

# SSL certificate
resource "google_compute_ssl_certificate" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name        = "${local.project_name}-${local.environment}-ssl-cert"
  private_key = var.ssl_private_key
  certificate = var.ssl_certificate
  
  description = "SSL certificate for ${local.project_name}"
  
  lifecycle {
    create_before_destroy = true
  }
}

# Modern SSL policy
resource "google_compute_ssl_policy" "modern" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name            = "${local.project_name}-${local.environment}-ssl-policy"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
  
  description = "Modern SSL policy for ${local.project_name}"
}

# URL map with advanced routing
resource "google_compute_url_map" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name            = "${local.project_name}-${local.environment}-url-map"
  default_service = google_compute_backend_service.api[0].id
  
  description = "URL map for ${local.project_name} with advanced routing"
  
  # API versioning routing
  dynamic "path_matcher" {
    for_each = var.enable_api_versioning ? [1] : []
    content {
      name            = "api-versioning"
      default_service = google_compute_backend_service.api[0].id
      
      path_rule {
        paths   = ["/api/v2/*"]
        service = google_compute_backend_service.api_v2[0].id
      }
      
      path_rule {
        paths   = ["/api/v1/*"]
        service = google_compute_backend_service.api[0].id
      }
    }
  }
  
  # Host rules for different environments or services
  dynamic "host_rule" {
    for_each = var.enable_api_versioning ? [1] : []
    content {
      hosts        = [var.api_hostname]
      path_matcher = "api-versioning"
    }
  }
}

# URL map for HTTP to HTTPS redirects
resource "google_compute_url_map" "redirect" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name = "${local.project_name}-${local.environment}-redirect-map"
  
  default_url_redirect {
    https_redirect         = true
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
    strip_query            = false
  }
  
  description = "URL map for HTTP to HTTPS redirects"
}

# Backend service for API
resource "google_compute_backend_service" "api" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name                    = "${local.project_name}-${local.environment}-api-backend"
  protocol                = "HTTP"
  port_name               = "http"
  timeout_sec            = 60
  enable_cdn             = var.enable_cdn
  session_affinity       = var.enable_sticky_sessions ? "CLIENT_IP" : "NONE"
  load_balancing_scheme  = "EXTERNAL"
  locality_lb_policy     = "ROUND_ROBIN"
  
  # Connection draining
  connection_draining_timeout_sec = var.environment == "prod" ? 300 : 60
  
  # Advanced health check
  health_checks = [google_compute_health_check.api[0].id]
  
  # Backend configuration
  dynamic "backend" {
    for_each = var.gcp_instance_groups
    content {
      group           = backend.value.instance_group
      balancing_mode  = "UTILIZATION"
      max_utilization = 0.8
      capacity_scaler = 1.0
    }
  }
  
  # CDN configuration
  dynamic "cdn_policy" {
    for_each = var.enable_cdn ? [1] : []
    content {
      cache_mode        = "CACHE_ALL_STATIC"
      default_ttl       = 3600
      max_ttl          = 86400
      client_ttl       = 3600
      negative_caching = true
      
      # Cache key policy
      cache_key_policy {
        include_host           = true
        include_protocol       = true
        include_query_string   = false
        query_string_whitelist = ["version", "format"]
      }
      
      # Negative caching policy
      negative_caching_policy {
        code = 404
        ttl  = 120
      }
      
      negative_caching_policy {
        code = 502
        ttl  = 60
      }
    }
  }
  
  # Circuit breaker
  outlier_detection {
    consecutive_errors                    = 3
    consecutive_gateway_errors           = 3
    interval                            = 30
    base_ejection_time                  = 30
    max_ejection_percent               = 50
    min_health_percent                 = 50
    split_external_local_origin_errors = true
  }
  
  # Logging configuration
  log_config {
    enable      = var.enable_access_logs
    sample_rate = var.environment == "prod" ? 0.1 : 1.0
  }
  
  # Security policy
  dynamic "security_policy" {
    for_each = var.enable_cloud_armor ? [google_compute_security_policy.main[0].id] : []
    content {
      security_policy = security_policy.value
    }
  }
  
  description = "Backend service for ${local.project_name} API"
}

# API v2 backend service for versioning
resource "google_compute_backend_service" "api_v2" {
  count = var.deploy_to_gcp && var.enable_api_versioning ? 1 : 0
  
  name                    = "${local.project_name}-${local.environment}-api-v2-backend"
  protocol                = "HTTP"
  port_name               = "http"
  timeout_sec            = 60
  enable_cdn             = var.enable_cdn
  session_affinity       = var.enable_sticky_sessions ? "CLIENT_IP" : "NONE"
  load_balancing_scheme  = "EXTERNAL"
  
  health_checks = [google_compute_health_check.api[0].id]
  
  dynamic "backend" {
    for_each = var.gcp_instance_groups
    content {
      group           = backend.value.instance_group
      balancing_mode  = "UTILIZATION"
      max_utilization = 0.8
      capacity_scaler = 1.0
    }
  }
  
  description = "Backend service for ${local.project_name} API v2"
}

# Advanced health check
resource "google_compute_health_check" "api" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name        = "${local.project_name}-${local.environment}-api-health-check"
  description = "Health check for ${local.project_name} API"
  
  timeout_sec         = local.health_check_config.timeout_seconds
  check_interval_sec  = local.health_check_config.interval_seconds
  healthy_threshold   = local.health_check_config.healthy_threshold
  unhealthy_threshold = local.health_check_config.unhealthy_threshold
  
  http_health_check {
    port               = local.health_check_config.port
    request_path       = local.health_check_config.path
    proxy_header       = "NONE"
    response           = ""
    port_specification = "USE_FIXED_PORT"
  }
  
  # Logging
  log_config {
    enable = var.enable_health_check_logs
  }
}

# Cloud Armor security policy
resource "google_compute_security_policy" "main" {
  count = var.deploy_to_gcp && var.enable_cloud_armor ? 1 : 0
  
  name        = "${local.project_name}-${local.environment}-security-policy"
  description = "Cloud Armor security policy for ${local.project_name}"
  
  # Default rule
  rule {
    action   = "allow"
    priority = "2147483647"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    description = "Default rule, allow all traffic"
  }
  
  # Rate limiting rule
  rule {
    action   = "throttle"
    priority = "1000"
    match {
      versioned_expr = "SRC_IPS_V1"
      config {
        src_ip_ranges = ["*"]
      }
    }
    rate_limit_options {
      conform_action = "allow"
      exceed_action  = "deny(429)"
      enforce_on_key = "IP"
      rate_limit_threshold {
        count        = var.rate_limit_requests_per_minute
        interval_sec = 60
      }
    }
    description = "Rate limiting rule"
  }
  
  # Block known bad IPs
  dynamic "rule" {
    for_each = var.blocked_ip_ranges
    content {
      action   = "deny(403)"
      priority = rule.key + 100
      match {
        versioned_expr = "SRC_IPS_V1"
        config {
          src_ip_ranges = [rule.value]
        }
      }
      description = "Block malicious IP range: ${rule.value}"
    }
  }
  
  # Geo-blocking (if enabled)
  dynamic "rule" {
    for_each = var.blocked_countries
    content {
      action   = "deny(403)"
      priority = rule.key + 500
      match {
        expr {
          expression = "origin.region_code == '${rule.value}'"
        }
      }
      description = "Block traffic from ${rule.value}"
    }
  }
  
  # Advanced threat protection
  adaptive_protection_config {
    layer_7_ddos_defense_config {
      enable = true
    }
    auto_deploy_config {
      load_threshold                = 0.1
      confidence_threshold         = 0.5
      impacted_baseline_threshold = 0.01
      expiration_sec              = 600
    }
  }
}

# ============================================================================
# GLOBAL TRAFFIC MANAGEMENT AND FAILOVER
# ============================================================================

# Health check for global traffic management
resource "kubernetes_config_map" "global_health_check" {
  metadata {
    name      = "${local.project_name}-global-health-config"
    namespace = var.kubernetes_namespace
    labels    = local.common_labels
  }
  
  data = {
    "health-check-config" = jsonencode({
      endpoints = {
        aws = {
          url = var.deploy_to_aws ? "https://${aws_lb.main[0].dns_name}${local.health_check_config.path}" : ""
          weight = local.traffic_weights.primary_region
          priority = 1
          health_check = {
            interval_seconds = local.health_check_config.interval_seconds
            timeout_seconds = local.health_check_config.timeout_seconds
            healthy_threshold = local.health_check_config.healthy_threshold
            unhealthy_threshold = local.health_check_config.unhealthy_threshold
          }
        }
        
        azure = {
          url = var.deploy_to_azure ? "https://${azurerm_public_ip.appgw[0].fqdn}${local.health_check_config.path}" : ""
          weight = local.traffic_weights.secondary_region
          priority = 2
          health_check = {
            interval_seconds = local.health_check_config.interval_seconds
            timeout_seconds = local.health_check_config.timeout_seconds
            healthy_threshold = local.health_check_config.healthy_threshold
            unhealthy_threshold = local.health_check_config.unhealthy_threshold
          }
        }
        
        gcp = {
          url = var.deploy_to_gcp ? "https://${google_compute_global_address.main[0].address}${local.health_check_config.path}" : ""
          weight = local.traffic_weights.tertiary_region
          priority = 3
          health_check = {
            interval_seconds = local.health_check_config.interval_seconds
            timeout_seconds = local.health_check_config.timeout_seconds
            healthy_threshold = local.health_check_config.healthy_threshold
            unhealthy_threshold = local.health_check_config.unhealthy_threshold
          }
        }
      }
      
      failover_policy = {
        enable_automatic_failover = true
        failover_threshold = 2  # Number of consecutive failures
        failback_delay_minutes = 5  # Wait before failing back to primary
        health_check_grace_period_seconds = local.health_check_config.grace_period_seconds
      }
      
      traffic_management = {
        enable_geo_routing = var.enable_geo_routing
        enable_latency_routing = var.enable_latency_routing
        enable_weighted_routing = true
        circuit_breaker = {
          failure_threshold = 5
          recovery_timeout_seconds = 60
          half_open_max_requests = 10
        }
      }
    })
    
    "monitoring-config" = jsonencode({
      metrics = {
        request_latency_percentiles = [50, 90, 95, 99]
        error_rate_threshold = 0.05  # 5% error rate
        availability_sla = 99.99     # 99.99% availability SLA
      }
      
      alerts = {
        endpoint_down = {
          enabled = true
          threshold = 3  # consecutive failures
          notification_channels = var.alert_notification_channels
        }
        
        high_error_rate = {
          enabled = true
          threshold = 0.1  # 10% error rate
          window_minutes = 5
          notification_channels = var.alert_notification_channels
        }
        
        high_latency = {
          enabled = true
          threshold_ms = var.latency_threshold_ms
          percentile = 95
          window_minutes = 5
          notification_channels = var.alert_notification_channels
        }
      }
    })
  }
}

# External DNS for global load balancing
resource "kubernetes_manifest" "external_dns_global" {
  count = var.enable_external_dns ? 1 : 0
  
  manifest = {
    apiVersion = "externaldns.k8s.io/v1alpha1"
    kind       = "DNSEndpoint"
    metadata = {
      name      = "${local.project_name}-global-dns"
      namespace = var.kubernetes_namespace
      labels    = local.common_labels
    }
    spec = {
      endpoints = concat(
        var.deploy_to_aws ? [{
          dnsName = var.global_domain_name
          recordType = "CNAME"
          targets = [aws_lb.main[0].dns_name]
          recordTTL = 60
          labels = {
            region = "aws"
            priority = "1"
          }
        }] : [],
        
        var.deploy_to_azure ? [{
          dnsName = var.global_domain_name
          recordType = "CNAME"
          targets = [azurerm_public_ip.appgw[0].fqdn]
          recordTTL = 60
          labels = {
            region = "azure"
            priority = "2"
          }
        }] : [],
        
        var.deploy_to_gcp ? [{
          dnsName = var.global_domain_name
          recordType = "A"
          targets = [google_compute_global_address.main[0].address]
          recordTTL = 60
          labels = {
            region = "gcp"
            priority = "3"
          }
        }] : []
      )
    }
  }
}