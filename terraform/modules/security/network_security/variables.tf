# =============================================================================
# NETWORK SECURITY SUBMODULE - VARIABLES
# Variables for Zero Trust network security configuration
# =============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# =============================================================================
# ZERO TRUST CONFIGURATION
# =============================================================================

variable "zero_trust_enabled" {
  description = "Enable Zero Trust network architecture"
  type        = bool
  default     = true
}

variable "micro_segmentation" {
  description = "Enable network micro-segmentation"
  type        = bool
  default     = true
}

# =============================================================================
# NETWORK CONFIGURATION
# =============================================================================

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the infrastructure"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default     = []
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for load balancers"
  type        = list(string)
  default     = []
}

# =============================================================================
# SECURITY FEATURES
# =============================================================================

variable "enable_waf" {
  description = "Enable Web Application Firewall"
  type        = bool
  default     = true
}

variable "enable_ddos_protection" {
  description = "Enable DDoS protection"
  type        = bool
  default     = true
}

variable "enable_network_acls" {
  description = "Enable Network Access Control Lists"
  type        = bool
  default     = true
}

# =============================================================================
# AWS SPECIFIC VARIABLES
# =============================================================================

variable "aws_vpc_id" {
  description = "AWS VPC ID"
  type        = string
  default     = null
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

# =============================================================================
# AZURE SPECIFIC VARIABLES
# =============================================================================

variable "azure_vnet_id" {
  description = "Azure Virtual Network ID"
  type        = string
  default     = null
}

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_gateway_subnet_id" {
  description = "Azure Application Gateway subnet ID"
  type        = string
  default     = null
}

variable "azure_gateway_subnet_cidr" {
  description = "Azure Application Gateway subnet CIDR"
  type        = string
  default     = "10.0.1.0/24"
}

variable "azure_app_subnet_cidr" {
  description = "Azure application subnet CIDR"
  type        = string
  default     = "10.0.10.0/24"
}

variable "azure_db_subnet_cidr" {
  description = "Azure database subnet CIDR"
  type        = string
  default     = "10.0.20.0/24"
}

# =============================================================================
# GCP SPECIFIC VARIABLES
# =============================================================================

variable "gcp_vpc_id" {
  description = "GCP VPC network name"
  type        = string
  default     = null
}

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

# =============================================================================
# WAF CONFIGURATION
# =============================================================================

variable "waf_rule_sets" {
  description = "WAF rule sets to enable"
  type        = list(string)
  default = [
    "AWSManagedRulesCommonRuleSet",
    "AWSManagedRulesKnownBadInputsRuleSet",
    "AWSManagedRulesSQLiRuleSet"
  ]
}

variable "rate_limit_requests_per_5min" {
  description = "Rate limit threshold (requests per 5 minutes)"
  type        = number
  default     = 2000
}

variable "blocked_countries" {
  description = "List of country codes to block"
  type        = list(string)
  default     = []
}

variable "enable_geo_blocking" {
  description = "Enable geographic blocking"
  type        = bool
  default     = false
}

# =============================================================================
# DDOS PROTECTION CONFIGURATION
# =============================================================================

variable "ddos_protection_tier" {
  description = "DDoS protection tier (Basic, Standard, Advanced)"
  type        = string
  default     = "Standard"
  
  validation {
    condition     = contains(["Basic", "Standard", "Advanced"], var.ddos_protection_tier)
    error_message = "DDoS protection tier must be one of: Basic, Standard, Advanced."
  }
}

variable "enable_ddos_rapid_response" {
  description = "Enable DDoS rapid response team support"
  type        = bool
  default     = false
}

# =============================================================================
# FIREWALL RULES CONFIGURATION
# =============================================================================

variable "custom_firewall_rules" {
  description = "Custom firewall rules to add"
  type = list(object({
    name        = string
    description = string
    direction   = string
    priority    = number
    action      = string
    protocol    = string
    ports       = list(string)
    source_cidr = list(string)
    target_tags = list(string)
  }))
  default = []
}

variable "enable_logging" {
  description = "Enable firewall rule logging"
  type        = bool
  default     = true
}

# =============================================================================
# NETWORK SEGMENTATION
# =============================================================================

variable "network_tiers" {
  description = "Network tier configuration"
  type = object({
    web_tier = object({
      allowed_ports = list(number)
      allowed_protocols = list(string)
    })
    app_tier = object({
      allowed_ports = list(number)
      allowed_protocols = list(string)
    })
    db_tier = object({
      allowed_ports = list(number)
      allowed_protocols = list(string)
    })
  })
  default = {
    web_tier = {
      allowed_ports = [80, 443]
      allowed_protocols = ["tcp"]
    }
    app_tier = {
      allowed_ports = [8080]
      allowed_protocols = ["tcp"]
    }
    db_tier = {
      allowed_ports = [5432]
      allowed_protocols = ["tcp"]
    }
  }
}

variable "enable_inter_subnet_communication" {
  description = "Allow communication between subnets"
  type        = bool
  default     = false
}

# =============================================================================
# MONITORING AND ALERTING
# =============================================================================

variable "enable_flow_logs" {
  description = "Enable VPC/VNet flow logs"
  type        = bool
  default     = true
}

variable "flow_logs_retention_days" {
  description = "Flow logs retention period in days"
  type        = number
  default     = 30
}

variable "enable_security_alerts" {
  description = "Enable security-related alerts"
  type        = bool
  default     = true
}

variable "alert_notification_targets" {
  description = "Notification targets for security alerts"
  type        = list(string)
  default     = []
}

# =============================================================================
# LOAD BALANCER CONFIGURATION
# =============================================================================

variable "enable_sticky_sessions" {
  description = "Enable sticky sessions on load balancer"
  type        = bool
  default     = false
}

variable "health_check_path" {
  description = "Health check path for load balancer targets"
  type        = string
  default     = "/health"
}

variable "health_check_interval" {
  description = "Health check interval in seconds"
  type        = number
  default     = 30
}

variable "health_check_timeout" {
  description = "Health check timeout in seconds"
  type        = number
  default     = 5
}

variable "health_check_healthy_threshold" {
  description = "Number of successful health checks before marking healthy"
  type        = number
  default     = 2
}

variable "health_check_unhealthy_threshold" {
  description = "Number of failed health checks before marking unhealthy"
  type        = number
  default     = 2
}

# =============================================================================
# SSL/TLS CONFIGURATION
# =============================================================================

variable "ssl_certificate_arn" {
  description = "SSL certificate ARN (AWS)"
  type        = string
  default     = ""
}

variable "ssl_certificate_id" {
  description = "SSL certificate ID (Azure)"
  type        = string
  default     = ""
}

variable "ssl_certificate_name" {
  description = "SSL certificate name (GCP)"
  type        = string
  default     = ""
}

variable "ssl_policy" {
  description = "SSL security policy"
  type        = string
  default     = "ELBSecurityPolicy-TLS-1-2-2017-01"
}

variable "enforce_https" {
  description = "Force HTTPS redirects"
  type        = bool
  default     = true
}

# =============================================================================
# COMPLIANCE CONFIGURATION
# =============================================================================

variable "compliance_frameworks" {
  description = "Compliance frameworks to adhere to"
  type        = list(string)
  default     = ["SOC2"]
}

variable "enable_compliance_logging" {
  description = "Enable compliance-specific logging"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Log retention period for compliance"
  type        = number
  default     = 2555  # 7 years
}

# =============================================================================
# COST OPTIMIZATION
# =============================================================================

variable "enable_nat_gateway_optimization" {
  description = "Optimize NAT Gateway usage for cost savings"
  type        = bool
  default     = true
}

variable "use_vpc_endpoints" {
  description = "Use VPC endpoints to reduce NAT Gateway costs"
  type        = bool
  default     = true
}

# =============================================================================
# ADVANCED SECURITY FEATURES
# =============================================================================

variable "enable_intrusion_detection" {
  description = "Enable network intrusion detection"
  type        = bool
  default     = true
}

variable "enable_threat_intelligence" {
  description = "Enable threat intelligence feeds"
  type        = bool
  default     = true
}

variable "enable_behavioral_analysis" {
  description = "Enable network behavioral analysis"
  type        = bool
  default     = false
}

variable "quarantine_subnet_id" {
  description = "Subnet ID for quarantining compromised resources"
  type        = string
  default     = null
}