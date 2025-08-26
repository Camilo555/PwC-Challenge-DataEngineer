# =============================================================================
# NETWORK SECURITY SUBMODULE - ZERO TRUST NETWORKING
# Advanced network security with micro-segmentation and WAF
# =============================================================================

terraform {
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
  }
}

# =============================================================================
# LOCAL VARIABLES
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Zero Trust network principles
  zero_trust_principles = {
    never_trust_always_verify = true
    least_privilege_access    = true
    assume_breach            = true
    explicit_verification    = true
  }
  
  # Common security group rules for Zero Trust
  zero_trust_ingress_rules = [
    {
      description = "HTTPS from allowed CIDRs only"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = var.allowed_cidr_blocks
    },
    {
      description = "HTTP redirect to HTTPS"
      from_port   = 80
      to_port     = 80
      protocol    = "tcp"
      cidr_blocks = var.allowed_cidr_blocks
    }
  ]
  
  zero_trust_egress_rules = [
    {
      description = "HTTPS outbound for updates and API calls"
      from_port   = 443
      to_port     = 443
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    },
    {
      description = "DNS outbound"
      from_port   = 53
      to_port     = 53
      protocol    = "udp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  ]
}

# =============================================================================
# AWS NETWORK SECURITY
# =============================================================================

# AWS Web Application Firewall (WAF)
resource "aws_wafv2_web_acl" "main" {
  count = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  
  name  = "${local.name_prefix}-waf"
  scope = "REGIONAL"
  
  default_action {
    allow {}
  }
  
  # OWASP Top 10 protection rules
  rule {
    name     = "AWSManagedRulesCommonRuleSet"
    priority = 1
    
    override_action {
      none {}
    }
    
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-CommonRuleSetMetric"
      sampled_requests_enabled   = true
    }
  }
  
  # Known bad inputs rule set
  rule {
    name     = "AWSManagedRulesKnownBadInputsRuleSet"
    priority = 2
    
    override_action {
      none {}
    }
    
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesKnownBadInputsRuleSet"
        vendor_name = "AWS"
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-BadInputsMetric"
      sampled_requests_enabled   = true
    }
  }
  
  # SQL injection protection
  rule {
    name     = "AWSManagedRulesSQLiRuleSet"
    priority = 3
    
    override_action {
      none {}
    }
    
    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesSQLiRuleSet"
        vendor_name = "AWS"
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-SQLiMetric"
      sampled_requests_enabled   = true
    }
  }
  
  # Rate limiting rule
  rule {
    name     = "RateLimitRule"
    priority = 4
    
    action {
      block {}
    }
    
    statement {
      rate_based_statement {
        limit              = 2000
        aggregate_key_type = "IP"
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-RateLimitMetric"
      sampled_requests_enabled   = true
    }
  }
  
  # Geo-blocking rule (example: block specific countries)
  rule {
    name     = "GeoBlockingRule"
    priority = 5
    
    action {
      block {}
    }
    
    statement {
      geo_match_statement {
        country_codes = ["CN", "RU", "KP"]  # Example blocked countries
      }
    }
    
    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${local.name_prefix}-GeoBlockMetric"
      sampled_requests_enabled   = true
    }
  }
  
  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "${local.name_prefix}-WAF"
    sampled_requests_enabled   = true
  }
  
  tags = var.tags
}

# AWS Shield Advanced (DDoS Protection)
resource "aws_shield_protection" "alb_protection" {
  count        = var.aws_vpc_id != null && var.enable_ddos_protection ? 1 : 0
  name         = "${local.name_prefix}-alb-shield"
  resource_arn = aws_lb.application_load_balancer[0].arn
}

# Application Load Balancer for WAF integration
resource "aws_lb" "application_load_balancer" {
  count              = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  name               = "${local.name_prefix}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb_sg[0].id]
  subnets           = var.public_subnet_ids
  
  enable_deletion_protection       = var.environment == "prod"
  enable_cross_zone_load_balancing = true
  enable_http2                    = true
  
  access_logs {
    bucket  = aws_s3_bucket.alb_logs[0].bucket
    enabled = true
  }
  
  tags = var.tags
}

# WAF Association with ALB
resource "aws_wafv2_web_acl_association" "alb_association" {
  count        = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  resource_arn = aws_lb.application_load_balancer[0].arn
  web_acl_arn  = aws_wafv2_web_acl.main[0].arn
}

# S3 bucket for ALB access logs
resource "aws_s3_bucket" "alb_logs" {
  count  = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  bucket = "${local.name_prefix}-alb-logs-${random_string.bucket_suffix[0].result}"
  
  tags = var.tags
}

resource "random_string" "bucket_suffix" {
  count   = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  length  = 8
  special = false
  upper   = false
}

resource "aws_s3_bucket_server_side_encryption_configuration" "alb_logs" {
  count  = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  bucket = aws_s3_bucket.alb_logs[0].id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "alb_logs" {
  count  = var.aws_vpc_id != null && var.enable_waf ? 1 : 0
  bucket = aws_s3_bucket.alb_logs[0].id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Security Groups for Zero Trust Architecture
resource "aws_security_group" "alb_sg" {
  count       = var.aws_vpc_id != null ? 1 : 0
  name        = "${local.name_prefix}-alb-sg"
  description = "Security group for Application Load Balancer"
  vpc_id      = var.aws_vpc_id
  
  dynamic "ingress" {
    for_each = local.zero_trust_ingress_rules
    content {
      description = ingress.value.description
      from_port   = ingress.value.from_port
      to_port     = ingress.value.to_port
      protocol    = ingress.value.protocol
      cidr_blocks = ingress.value.cidr_blocks
    }
  }
  
  dynamic "egress" {
    for_each = local.zero_trust_egress_rules
    content {
      description = egress.value.description
      from_port   = egress.value.from_port
      to_port     = egress.value.to_port
      protocol    = egress.value.protocol
      cidr_blocks = egress.value.cidr_blocks
    }
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-alb-sg"
    Type = "LoadBalancer"
  })
}

# Security group for application tier
resource "aws_security_group" "app_sg" {
  count       = var.aws_vpc_id != null ? 1 : 0
  name        = "${local.name_prefix}-app-sg"
  description = "Security group for application tier - Zero Trust"
  vpc_id      = var.aws_vpc_id
  
  # Only allow traffic from ALB
  ingress {
    description     = "HTTP from ALB only"
    from_port       = 8080
    to_port         = 8080
    protocol        = "tcp"
    security_groups = [aws_security_group.alb_sg[0].id]
  }
  
  # Egress for database and external APIs
  egress {
    description = "PostgreSQL to database subnet"
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.private_subnet_cidrs
  }
  
  egress {
    description = "HTTPS for external API calls"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  egress {
    description = "DNS resolution"
    from_port   = 53
    to_port     = 53
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-app-sg"
    Type = "Application"
  })
}

# Security group for database tier
resource "aws_security_group" "db_sg" {
  count       = var.aws_vpc_id != null ? 1 : 0
  name        = "${local.name_prefix}-db-sg"
  description = "Security group for database tier - Zero Trust"
  vpc_id      = var.aws_vpc_id
  
  # Only allow database traffic from application tier
  ingress {
    description     = "PostgreSQL from application tier only"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.app_sg[0].id]
  }
  
  # No outbound internet access for database
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-db-sg"
    Type = "Database"
  })
}

# Network ACLs for additional layer of security
resource "aws_network_acl" "private_nacl" {
  count  = var.aws_vpc_id != null && var.enable_network_acls ? 1 : 0
  vpc_id = var.aws_vpc_id
  
  # Allow inbound HTTPS from public subnets
  ingress {
    protocol   = "tcp"
    rule_no    = 100
    action     = "allow"
    cidr_block = "10.0.0.0/16"
    from_port  = 8080
    to_port    = 8080
  }
  
  # Allow database traffic within private subnets
  ingress {
    protocol   = "tcp"
    rule_no    = 200
    action     = "allow"
    cidr_block = "10.0.0.0/16"
    from_port  = 5432
    to_port    = 5432
  }
  
  # Allow ephemeral ports for return traffic
  ingress {
    protocol   = "tcp"
    rule_no    = 300
    action     = "allow"
    cidr_block = "0.0.0.0/0"
    from_port  = 1024
    to_port    = 65535
  }
  
  # Outbound rules
  egress {
    protocol   = "tcp"
    rule_no    = 100
    action     = "allow"
    cidr_block = "0.0.0.0/0"
    from_port  = 443
    to_port    = 443
  }
  
  egress {
    protocol   = "tcp"
    rule_no    = 200
    action     = "allow"
    cidr_block = "10.0.0.0/16"
    from_port  = 5432
    to_port    = 5432
  }
  
  egress {
    protocol   = "udp"
    rule_no    = 300
    action     = "allow"
    cidr_block = "0.0.0.0/0"
    from_port  = 53
    to_port    = 53
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-private-nacl"
  })
}

# =============================================================================
# AZURE NETWORK SECURITY
# =============================================================================

# Azure Application Gateway with WAF
resource "azurerm_application_gateway" "main" {
  count               = var.azure_vnet_id != null && var.enable_waf ? 1 : 0
  name                = "${local.name_prefix}-appgw"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  
  sku {
    name     = "WAF_v2"
    tier     = "WAF_v2"
    capacity = 2
  }
  
  waf_configuration {
    enabled          = true
    firewall_mode    = "Prevention"
    rule_set_type    = "OWASP"
    rule_set_version = "3.2"
    
    disabled_rule_group {
      rule_group_name = "REQUEST-920-PROTOCOL-ENFORCEMENT"
      rules          = [920300, 920440]
    }
  }
  
  gateway_ip_configuration {
    name      = "gateway-ip-config"
    subnet_id = var.azure_gateway_subnet_id
  }
  
  frontend_port {
    name = "https-port"
    port = 443
  }
  
  frontend_port {
    name = "http-port"
    port = 80
  }
  
  frontend_ip_configuration {
    name                 = "frontend-ip-config"
    public_ip_address_id = azurerm_public_ip.gateway_pip[0].id
  }
  
  backend_address_pool {
    name = "backend-pool"
  }
  
  backend_http_settings {
    name                  = "http-settings"
    cookie_based_affinity = "Disabled"
    path                  = "/"
    port                  = 8080
    protocol              = "Http"
    request_timeout       = 60
  }
  
  http_listener {
    name                           = "https-listener"
    frontend_ip_configuration_name = "frontend-ip-config"
    frontend_port_name            = "https-port"
    protocol                      = "Https"
    ssl_certificate_name          = "ssl-cert"
  }
  
  http_listener {
    name                           = "http-listener"
    frontend_ip_configuration_name = "frontend-ip-config"
    frontend_port_name            = "http-port"
    protocol                      = "Http"
  }
  
  # SSL certificate (placeholder - should be replaced with actual certificate)
  ssl_certificate {
    name     = "ssl-cert"
    data     = filebase64("${path.module}/dummy-cert.pfx")
    password = "dummy-password"
  }
  
  request_routing_rule {
    name                       = "https-rule"
    rule_type                 = "Basic"
    http_listener_name        = "https-listener"
    backend_address_pool_name = "backend-pool"
    backend_http_settings_name = "http-settings"
  }
  
  # Redirect HTTP to HTTPS
  request_routing_rule {
    name               = "http-redirect-rule"
    rule_type         = "Basic"
    http_listener_name = "http-listener"
    redirect_configuration_name = "http-to-https-redirect"
  }
  
  redirect_configuration {
    name                 = "http-to-https-redirect"
    redirect_type        = "Permanent"
    target_listener_name = "https-listener"
    include_path         = true
    include_query_string = true
  }
  
  tags = var.tags
}

resource "azurerm_public_ip" "gateway_pip" {
  count               = var.azure_vnet_id != null && var.enable_waf ? 1 : 0
  name                = "${local.name_prefix}-gateway-pip"
  resource_group_name = var.azure_resource_group_name
  location           = var.azure_location
  allocation_method   = "Static"
  sku                = "Standard"
  
  tags = var.tags
}

# Network Security Groups for Azure
resource "azurerm_network_security_group" "app_nsg" {
  count               = var.azure_vnet_id != null ? 1 : 0
  name                = "${local.name_prefix}-app-nsg"
  location           = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  # Allow HTTPS from Application Gateway
  security_rule {
    name                       = "Allow-HTTPS-From-Gateway"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8080"
    source_address_prefix      = var.azure_gateway_subnet_cidr
    destination_address_prefix = "*"
  }
  
  # Allow database connectivity
  security_rule {
    name                       = "Allow-Database-Outbound"
    priority                   = 100
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = "*"
    destination_address_prefix = var.azure_db_subnet_cidr
  }
  
  # Allow HTTPS outbound
  security_rule {
    name                       = "Allow-HTTPS-Outbound"
    priority                   = 200
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  # Deny all other traffic
  security_rule {
    name                       = "Deny-All-Inbound"
    priority                   = 4000
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = var.tags
}

resource "azurerm_network_security_group" "db_nsg" {
  count               = var.azure_vnet_id != null ? 1 : 0
  name                = "${local.name_prefix}-db-nsg"
  location           = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  # Allow database traffic from app subnet only
  security_rule {
    name                       = "Allow-Database-From-App"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = var.azure_app_subnet_cidr
    destination_address_prefix = "*"
  }
  
  # Deny all other traffic
  security_rule {
    name                       = "Deny-All-Inbound"
    priority                   = 4000
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  security_rule {
    name                       = "Deny-All-Outbound"
    priority                   = 4000
    direction                  = "Outbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
  
  tags = var.tags
}

# Azure DDoS Protection Plan
resource "azurerm_network_ddos_protection_plan" "main" {
  count               = var.azure_vnet_id != null && var.enable_ddos_protection ? 1 : 0
  name                = "${local.name_prefix}-ddos-plan"
  location           = var.azure_location
  resource_group_name = var.azure_resource_group_name
  
  tags = var.tags
}

# =============================================================================
# GCP NETWORK SECURITY
# =============================================================================

# Google Cloud Armor (WAF and DDoS protection)
resource "google_compute_security_policy" "main" {
  count   = var.gcp_vpc_id != null && var.enable_waf ? 1 : 0
  name    = "${local.name_prefix}-security-policy"
  project = var.gcp_project_id
  
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
    description = "default rule"
  }
  
  # Rate limiting rule
  rule {
    action   = "rate_based_ban"
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
        count        = 100
        interval_sec = 60
      }
      ban_duration_sec = 600
    }
    description = "Rate limiting rule"
  }
  
  # SQL injection protection
  rule {
    action   = "deny(403)"
    priority = "2000"
    match {
      expr {
        expression = "evaluatePreconfiguredExpr('sqli-stable')"
      }
    }
    description = "SQL injection protection"
  }
  
  # XSS protection
  rule {
    action   = "deny(403)"
    priority = "3000"
    match {
      expr {
        expression = "evaluatePreconfiguredExpr('xss-stable')"
      }
    }
    description = "XSS protection"
  }
  
  # Geographic restriction (example)
  rule {
    action   = "deny(403)"
    priority = "4000"
    match {
      expr {
        expression = "origin.region_code == 'CN' || origin.region_code == 'RU'"
      }
    }
    description = "Block traffic from specific countries"
  }
}

# GCP Firewall Rules for Zero Trust
resource "google_compute_firewall" "allow_https" {
  count     = var.gcp_vpc_id != null ? 1 : 0
  name      = "${local.name_prefix}-allow-https"
  network   = var.gcp_vpc_id
  project   = var.gcp_project_id
  direction = "INGRESS"
  
  allow {
    protocol = "tcp"
    ports    = ["443"]
  }
  
  source_ranges = var.allowed_cidr_blocks
  target_tags   = ["https-server"]
}

resource "google_compute_firewall" "allow_http_redirect" {
  count     = var.gcp_vpc_id != null ? 1 : 0
  name      = "${local.name_prefix}-allow-http-redirect"
  network   = var.gcp_vpc_id
  project   = var.gcp_project_id
  direction = "INGRESS"
  
  allow {
    protocol = "tcp"
    ports    = ["80"]
  }
  
  source_ranges = var.allowed_cidr_blocks
  target_tags   = ["http-redirect"]
}

resource "google_compute_firewall" "app_to_db" {
  count     = var.gcp_vpc_id != null ? 1 : 0
  name      = "${local.name_prefix}-app-to-db"
  network   = var.gcp_vpc_id
  project   = var.gcp_project_id
  direction = "INGRESS"
  
  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }
  
  source_tags = ["app-server"]
  target_tags = ["db-server"]
}

# Deny all other traffic (implicit in GCP, but explicit for clarity)
resource "google_compute_firewall" "deny_all" {
  count     = var.gcp_vpc_id != null ? 1 : 0
  name      = "${local.name_prefix}-deny-all"
  network   = var.gcp_vpc_id
  project   = var.gcp_project_id
  direction = "INGRESS"
  priority  = 65534
  
  deny {
    protocol = "all"
  }
  
  source_ranges = ["0.0.0.0/0"]
}

# =============================================================================
# OUTPUTS
# =============================================================================

output "security_group_ids" {
  description = "Security group IDs for different tiers"
  value = {
    aws = var.aws_vpc_id != null ? {
      alb_sg = aws_security_group.alb_sg[0].id
      app_sg = aws_security_group.app_sg[0].id
      db_sg  = aws_security_group.db_sg[0].id
    } : null
    
    azure = var.azure_vnet_id != null ? {
      app_nsg = azurerm_network_security_group.app_nsg[0].id
      db_nsg  = azurerm_network_security_group.db_nsg[0].id
    } : null
    
    gcp = var.gcp_vpc_id != null ? {
      firewall_rules = [
        google_compute_firewall.allow_https[0].id,
        google_compute_firewall.allow_http_redirect[0].id,
        google_compute_firewall.app_to_db[0].id
      ]
    } : null
  }
}

output "waf_web_acl_arn" {
  description = "WAF Web ACL ARN"
  value       = var.aws_vpc_id != null && var.enable_waf ? aws_wafv2_web_acl.main[0].arn : null
}

output "application_gateway_id" {
  description = "Azure Application Gateway ID"
  value       = var.azure_vnet_id != null && var.enable_waf ? azurerm_application_gateway.main[0].id : null
}

output "cloud_armor_policy" {
  description = "GCP Cloud Armor security policy"
  value       = var.gcp_vpc_id != null && var.enable_waf ? google_compute_security_policy.main[0].id : null
}

output "network_acl_ids" {
  description = "Network ACL IDs"
  value = {
    aws_private_nacl = var.aws_vpc_id != null && var.enable_network_acls ? aws_network_acl.private_nacl[0].id : null
  }
}

output "firewall_rule_ids" {
  description = "Firewall rule IDs"
  value = var.gcp_vpc_id != null ? [
    google_compute_firewall.allow_https[0].id,
    google_compute_firewall.allow_http_redirect[0].id,
    google_compute_firewall.app_to_db[0].id,
    google_compute_firewall.deny_all[0].id
  ] : []
}

output "ddos_protection_id" {
  description = "DDoS protection resource IDs"
  value = {
    aws_shield   = var.aws_vpc_id != null && var.enable_ddos_protection ? aws_shield_protection.alb_protection[0].id : null
    azure_ddos   = var.azure_vnet_id != null && var.enable_ddos_protection ? azurerm_network_ddos_protection_plan.main[0].id : null
  }
}

output "segmentation_policies" {
  description = "Network segmentation policies applied"
  value = {
    zero_trust_enabled = var.zero_trust_enabled
    micro_segmentation = var.micro_segmentation
    
    policies = {
      web_tier_isolation    = "Only HTTPS/HTTP traffic allowed from internet"
      app_tier_isolation    = "Only traffic from load balancer allowed"
      db_tier_isolation     = "Only traffic from application tier allowed"
      no_lateral_movement   = "Cross-tier communication blocked"
      egress_restrictions   = "Outbound traffic limited to required services"
    }
  }
}

output "micro_segmentation_enabled" {
  description = "Whether micro-segmentation is enabled"
  value       = var.micro_segmentation
}