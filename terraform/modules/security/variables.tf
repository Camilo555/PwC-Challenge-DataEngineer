# =============================================================================
# ENTERPRISE SECURITY MODULE - VARIABLES
# Comprehensive security configuration variables
# =============================================================================

# =============================================================================
# BASIC CONFIGURATION
# =============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
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
  description = "Labels to apply to all resources"
  type        = map(string)
  default     = {}
}

# =============================================================================
# MULTI-CLOUD DEPLOYMENT
# =============================================================================

variable "deploy_to_aws" {
  description = "Deploy AWS security resources"
  type        = bool
  default     = false
}

variable "deploy_to_azure" {
  description = "Deploy Azure security resources"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Deploy GCP security resources"
  type        = bool
  default     = false
}

# Cloud resource IDs for security integration
variable "aws_vpc_id" {
  description = "AWS VPC ID for security group configuration"
  type        = string
  default     = null
}

variable "azure_vnet_id" {
  description = "Azure Virtual Network ID for NSG configuration"
  type        = string
  default     = null
}

variable "gcp_vpc_id" {
  description = "GCP VPC ID for firewall configuration"
  type        = string
  default     = null
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDR blocks for network segmentation"
  type        = list(string)
  default     = []
}

# =============================================================================
# NETWORK SECURITY
# =============================================================================

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to access the infrastructure"
  type        = list(string)
  default     = []
}

variable "enable_network_segmentation" {
  description = "Enable network micro-segmentation"
  type        = bool
  default     = true
}

variable "enable_ddos_protection" {
  description = "Enable DDoS protection"
  type        = bool
  default     = true
}

variable "enable_waf" {
  description = "Enable Web Application Firewall"
  type        = bool
  default     = true
}

# =============================================================================
# ENCRYPTION AND DATA PROTECTION
# =============================================================================

variable "enable_encryption" {
  description = "Enable encryption at rest and in transit"
  type        = bool
  default     = true
}

variable "enable_field_level_encryption" {
  description = "Enable field-level encryption for sensitive data"
  type        = bool
  default     = false
}

variable "key_rotation_days" {
  description = "Number of days between key rotations"
  type        = number
  default     = 90
  
  validation {
    condition     = var.key_rotation_days >= 30 && var.key_rotation_days <= 365
    error_message = "Key rotation days must be between 30 and 365."
  }
}

# =============================================================================
# SECRETS MANAGEMENT
# =============================================================================

variable "secrets_backend" {
  description = "Secrets management backend"
  type        = string
  default     = "aws-secrets-manager"
  
  validation {
    condition = contains([
      "aws-secrets-manager",
      "azure-key-vault",
      "gcp-secret-manager",
      "hashicorp-vault"
    ], var.secrets_backend)
    error_message = "Secrets backend must be one of: aws-secrets-manager, azure-key-vault, gcp-secret-manager, hashicorp-vault."
  }
}

variable "enable_automatic_rotation" {
  description = "Enable automatic secret rotation"
  type        = bool
  default     = true
}

variable "secret_retention_days" {
  description = "Number of days to retain deleted secrets"
  type        = number
  default     = 30
}

# =============================================================================
# IDENTITY AND ACCESS MANAGEMENT
# =============================================================================

variable "enable_rbac" {
  description = "Enable Role-Based Access Control"
  type        = bool
  default     = true
}

variable "enable_just_in_time_access" {
  description = "Enable Just-In-Time access for privileged operations"
  type        = bool
  default     = true
}

variable "enable_privileged_access_management" {
  description = "Enable Privileged Access Management (PAM)"
  type        = bool
  default     = true
}

variable "enable_conditional_access" {
  description = "Enable conditional access policies"
  type        = bool
  default     = true
}

variable "session_timeout_minutes" {
  description = "Session timeout in minutes"
  type        = number
  default     = 60
}

variable "password_policy" {
  description = "Password policy configuration"
  type = object({
    min_length              = number
    require_uppercase       = bool
    require_lowercase       = bool
    require_numbers         = bool
    require_symbols         = bool
    max_age_days           = number
    history_count          = number
    lockout_threshold      = number
    lockout_duration_minutes = number
  })
  default = {
    min_length              = 12
    require_uppercase       = true
    require_lowercase       = true
    require_numbers         = true
    require_symbols         = true
    max_age_days           = 90
    history_count          = 12
    lockout_threshold      = 5
    lockout_duration_minutes = 30
  }
}

# =============================================================================
# AUDIT LOGGING AND MONITORING
# =============================================================================

variable "enable_audit_logs" {
  description = "Enable comprehensive audit logging"
  type        = bool
  default     = true
}

variable "audit_log_retention_days" {
  description = "Audit log retention period in days"
  type        = number
  default     = 2555  # 7 years for compliance
}

variable "enable_real_time_monitoring" {
  description = "Enable real-time security monitoring"
  type        = bool
  default     = true
}

variable "enable_anomaly_detection" {
  description = "Enable ML-based anomaly detection"
  type        = bool
  default     = true
}

variable "enable_user_behavior_analytics" {
  description = "Enable User and Entity Behavior Analytics (UEBA)"
  type        = bool
  default     = true
}

# =============================================================================
# COMPLIANCE FRAMEWORKS
# =============================================================================

variable "compliance_frameworks" {
  description = "List of compliance frameworks to implement"
  type        = list(string)
  default     = ["SOC2"]
  
  validation {
    condition = length(setintersection(var.compliance_frameworks, [
      "SOC2", "GDPR", "HIPAA", "PCI-DSS", "ISO27001", "NIST", "FedRAMP"
    ])) == length(var.compliance_frameworks)
    error_message = "Compliance frameworks must be from: SOC2, GDPR, HIPAA, PCI-DSS, ISO27001, NIST, FedRAMP."
  }
}

variable "enable_compliance_reporting" {
  description = "Enable automated compliance reporting"
  type        = bool
  default     = true
}

variable "compliance_scan_schedule" {
  description = "Cron schedule for compliance scans"
  type        = string
  default     = "0 2 * * *"  # Daily at 2 AM
}

# =============================================================================
# VULNERABILITY MANAGEMENT
# =============================================================================

variable "enable_security_scanning" {
  description = "Enable security vulnerability scanning"
  type        = bool
  default     = true
}

variable "enable_vulnerability_assessment" {
  description = "Enable automated vulnerability assessments"
  type        = bool
  default     = true
}

variable "security_scan_schedule" {
  description = "Cron schedule for security scans"
  type        = string
  default     = "0 1 * * *"  # Daily at 1 AM
}

variable "vulnerability_severity_threshold" {
  description = "Minimum vulnerability severity to report (LOW, MEDIUM, HIGH, CRITICAL)"
  type        = string
  default     = "MEDIUM"
  
  validation {
    condition     = contains(["LOW", "MEDIUM", "HIGH", "CRITICAL"], var.vulnerability_severity_threshold)
    error_message = "Vulnerability severity threshold must be one of: LOW, MEDIUM, HIGH, CRITICAL."
  }
}

variable "enable_container_scanning" {
  description = "Enable container image vulnerability scanning"
  type        = bool
  default     = true
}

variable "enable_infrastructure_scanning" {
  description = "Enable infrastructure configuration scanning"
  type        = bool
  default     = true
}

variable "enable_secrets_scanning" {
  description = "Enable secrets detection in code and configuration"
  type        = bool
  default     = true
}

# =============================================================================
# INCIDENT RESPONSE
# =============================================================================

variable "enable_incident_response" {
  description = "Enable automated incident response"
  type        = bool
  default     = true
}

variable "enable_automated_containment" {
  description = "Enable automated threat containment"
  type        = bool
  default     = false
}

variable "security_contact_email" {
  description = "Email address for security notifications"
  type        = string
  default     = ""
}

variable "escalation_webhook_url" {
  description = "Webhook URL for security incident escalation"
  type        = string
  default     = ""
  sensitive   = true
}

variable "breach_notification_required" {
  description = "Enable automatic breach notification"
  type        = bool
  default     = false
}

# =============================================================================
# THREAT INTELLIGENCE
# =============================================================================

variable "enable_threat_intelligence" {
  description = "Enable threat intelligence feeds"
  type        = bool
  default     = true
}

variable "threat_intel_feeds" {
  description = "List of threat intelligence feed sources"
  type        = list(string)
  default = [
    "misp",
    "otx",
    "crowdstrike",
    "virustotal"
  ]
}

variable "enable_threat_hunting" {
  description = "Enable proactive threat hunting"
  type        = bool
  default     = false
}

# =============================================================================
# DATA PRIVACY AND PROTECTION
# =============================================================================

variable "enable_data_classification" {
  description = "Enable automated data classification"
  type        = bool
  default     = true
}

variable "enable_data_loss_prevention" {
  description = "Enable Data Loss Prevention (DLP)"
  type        = bool
  default     = true
}

variable "enable_pii_detection" {
  description = "Enable Personally Identifiable Information (PII) detection"
  type        = bool
  default     = true
}

variable "enable_data_lineage" {
  description = "Enable data lineage tracking for privacy compliance"
  type        = bool
  default     = true
}

variable "enable_consent_management" {
  description = "Enable consent management for GDPR compliance"
  type        = bool
  default     = false
}

variable "data_retention_policies" {
  description = "Data retention policies by data classification"
  type = map(object({
    retention_days = number
    auto_delete    = bool
    archive_after_days = number
  }))
  default = {
    public = {
      retention_days     = 365
      auto_delete       = true
      archive_after_days = 90
    }
    internal = {
      retention_days     = 2555  # 7 years
      auto_delete       = false
      archive_after_days = 365
    }
    confidential = {
      retention_days     = 2555  # 7 years
      auto_delete       = false
      archive_after_days = 365
    }
    restricted = {
      retention_days     = 2555  # 7 years
      auto_delete       = false
      archive_after_days = 180
    }
  }
}

# =============================================================================
# BACKUP AND DISASTER RECOVERY
# =============================================================================

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery capabilities"
  type        = bool
  default     = false
}

variable "enable_backup_encryption" {
  description = "Enable backup encryption"
  type        = bool
  default     = true
}

variable "enable_immutable_backups" {
  description = "Enable immutable backup storage"
  type        = bool
  default     = false
}

variable "backup_retention_years" {
  description = "Backup retention period in years"
  type        = number
  default     = 7
}

# =============================================================================
# SERVICE MESH SECURITY
# =============================================================================

variable "enable_service_mesh" {
  description = "Enable service mesh (Istio) for Zero Trust networking"
  type        = bool
  default     = true
}

variable "enable_mtls" {
  description = "Enable mutual TLS (mTLS) for service-to-service communication"
  type        = bool
  default     = true
}

variable "enable_workload_identity" {
  description = "Enable workload identity for service authentication"
  type        = bool
  default     = true
}

variable "service_mesh_mode" {
  description = "Service mesh mode (strict, permissive)"
  type        = string
  default     = "strict"
  
  validation {
    condition     = contains(["strict", "permissive"], var.service_mesh_mode)
    error_message = "Service mesh mode must be either 'strict' or 'permissive'."
  }
}

# =============================================================================
# NOTIFICATION AND ALERTING
# =============================================================================

variable "slack_webhook_url" {
  description = "Slack webhook URL for security alerts"
  type        = string
  default     = ""
  sensitive   = true
}

variable "security_notifications_email" {
  description = "Email addresses for security notifications"
  type        = list(string)
  default     = []
}

variable "enable_sms_alerts" {
  description = "Enable SMS alerts for critical security events"
  type        = bool
  default     = false
}

variable "alert_severity_threshold" {
  description = "Minimum alert severity for notifications (LOW, MEDIUM, HIGH, CRITICAL)"
  type        = string
  default     = "HIGH"
  
  validation {
    condition     = contains(["LOW", "MEDIUM", "HIGH", "CRITICAL"], var.alert_severity_threshold)
    error_message = "Alert severity threshold must be one of: LOW, MEDIUM, HIGH, CRITICAL."
  }
}

# =============================================================================
# COST AND RESOURCE OPTIMIZATION
# =============================================================================

variable "enable_cost_optimization" {
  description = "Enable cost optimization for security resources"
  type        = bool
  default     = true
}

variable "security_budget_limit" {
  description = "Monthly budget limit for security resources (USD)"
  type        = number
  default     = 10000
}

variable "enable_resource_right_sizing" {
  description = "Enable automatic resource right-sizing"
  type        = bool
  default     = true
}

# =============================================================================
# ADVANCED SECURITY FEATURES
# =============================================================================

variable "enable_zero_trust" {
  description = "Enable Zero Trust architecture"
  type        = bool
  default     = true
}

variable "enable_deception_technology" {
  description = "Enable deception technology (honeypots, canaries)"
  type        = bool
  default     = false
}

variable "enable_security_orchestration" {
  description = "Enable Security Orchestration, Automation and Response (SOAR)"
  type        = bool
  default     = false
}

variable "enable_extended_detection" {
  description = "Enable Extended Detection and Response (XDR)"
  type        = bool
  default     = false
}

variable "enable_cloud_security_posture" {
  description = "Enable Cloud Security Posture Management (CSPM)"
  type        = bool
  default     = true
}

# =============================================================================
# INTEGRATION SETTINGS
# =============================================================================

variable "external_siem_endpoint" {
  description = "External SIEM system endpoint for log forwarding"
  type        = string
  default     = ""
}

variable "external_vault_endpoint" {
  description = "External HashiCorp Vault endpoint"
  type        = string
  default     = ""
}

variable "splunk_hec_endpoint" {
  description = "Splunk HTTP Event Collector endpoint"
  type        = string
  default     = ""
  sensitive   = true
}

variable "datadog_api_key" {
  description = "Datadog API key for security monitoring integration"
  type        = string
  default     = ""
  sensitive   = true
}

variable "pagerduty_integration_key" {
  description = "PagerDuty integration key for incident response"
  type        = string
  default     = ""
  sensitive   = true
}