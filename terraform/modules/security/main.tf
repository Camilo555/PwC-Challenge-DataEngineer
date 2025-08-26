# =============================================================================
# ENTERPRISE SECURITY MODULE - ZERO TRUST ARCHITECTURE
# Multi-cloud security implementation with advanced compliance frameworks
# =============================================================================

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
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    vault = {
      source  = "hashicorp/vault"
      version = "~> 3.20"
    }
  }
}

# =============================================================================
# LOCAL VARIABLES AND CONFIGURATIONS
# =============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # Zero Trust security policies
  zero_trust_policies = {
    verify_explicitly     = true
    least_privilege      = true
    assume_breach        = true
    continuous_validation = true
    device_compliance    = true
    network_segmentation = true
  }
  
  # Compliance framework mappings
  compliance_controls = {
    soc2 = {
      encryption_required    = true
      audit_logging_required = true
      access_reviews_required = true
      vulnerability_scanning  = true
      incident_response      = true
    }
    gdpr = {
      data_minimization     = true
      consent_management    = true
      right_to_erasure     = true
      data_portability     = true
      breach_notification  = true
      privacy_by_design    = true
    }
    hipaa = {
      phi_encryption       = true
      access_controls      = true
      audit_controls       = true
      integrity_controls   = true
      transmission_security = true
      assigned_security_responsibility = true
    }
    pci_dss = {
      cardholder_data_protection = true
      secure_network             = true
      vulnerability_management    = true
      access_control_measures     = true
      network_monitoring         = true
      information_security_policy = true
    }
  }
  
  # Security scanning configurations
  security_scanning = {
    container_scanning    = var.enable_security_scanning
    infrastructure_scanning = var.enable_vulnerability_assessment
    code_scanning        = var.enable_security_scanning
    secrets_scanning     = true
    dependency_scanning  = true
    license_scanning     = true
  }
  
  common_tags = merge(var.labels, {
    SecurityModule = "enterprise-zero-trust"
    ComplianceFrameworks = join(",", var.compliance_frameworks)
    SecurityLevel = "high"
  })
}

# =============================================================================
# CERTIFICATE AUTHORITY AND PKI INFRASTRUCTURE
# =============================================================================

# Root CA Certificate for Zero Trust
resource "tls_private_key" "root_ca" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_self_signed_cert" "root_ca" {
  private_key_pem = tls_private_key.root_ca.private_key_pem

  subject {
    common_name  = "${local.name_prefix}-root-ca"
    organization = "PwC Retail Data Platform"
    country      = "US"
  }

  validity_period_hours = 87600 # 10 years
  is_ca_certificate     = true

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "cert_signing",
    "crl_signing",
  ]
}

# Intermediate CA for service certificates
resource "tls_private_key" "intermediate_ca" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

resource "tls_cert_request" "intermediate_ca" {
  private_key_pem = tls_private_key.intermediate_ca.private_key_pem

  subject {
    common_name  = "${local.name_prefix}-intermediate-ca"
    organization = "PwC Retail Data Platform"
    country      = "US"
  }
}

resource "tls_locally_signed_cert" "intermediate_ca" {
  cert_request_pem   = tls_cert_request.intermediate_ca.cert_request_pem
  ca_private_key_pem = tls_private_key.root_ca.private_key_pem
  ca_cert_pem        = tls_self_signed_cert.root_ca.cert_pem

  validity_period_hours = 43800 # 5 years
  is_ca_certificate     = true

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "cert_signing",
    "crl_signing",
  ]
}

# =============================================================================
# MULTI-CLOUD SECRETS MANAGEMENT
# =============================================================================

# AWS Secrets Manager integration
module "aws_secrets" {
  count  = var.deploy_to_aws ? 1 : 0
  source = "./aws_secrets"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Certificate storage
  root_ca_cert        = tls_self_signed_cert.root_ca.cert_pem
  root_ca_key         = tls_private_key.root_ca.private_key_pem
  intermediate_ca_cert = tls_locally_signed_cert.intermediate_ca.cert_pem
  intermediate_ca_key  = tls_private_key.intermediate_ca.private_key_pem
  
  # Compliance requirements
  enable_automatic_rotation = contains(var.compliance_frameworks, "SOC2")
  retention_days           = var.environment == "prod" ? 2555 : 90
  
  tags = local.common_tags
}

# Azure Key Vault integration
module "azure_secrets" {
  count  = var.deploy_to_azure ? 1 : 0
  source = "./azure_secrets"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Certificate storage
  root_ca_cert        = tls_self_signed_cert.root_ca.cert_pem
  root_ca_key         = tls_private_key.root_ca.private_key_pem
  intermediate_ca_cert = tls_locally_signed_cert.intermediate_ca.cert_pem
  intermediate_ca_key  = tls_private_key.intermediate_ca.private_key_pem
  
  # Compliance requirements
  enable_hsm                = contains(var.compliance_frameworks, "HIPAA")
  enable_purge_protection   = var.environment == "prod"
  
  tags = local.common_tags
}

# Google Secret Manager integration
module "gcp_secrets" {
  count  = var.deploy_to_gcp ? 1 : 0
  source = "./gcp_secrets"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Certificate storage
  root_ca_cert        = tls_self_signed_cert.root_ca.cert_pem
  root_ca_key         = tls_private_key.root_ca.private_key_pem
  intermediate_ca_cert = tls_locally_signed_cert.intermediate_ca.cert_pem
  intermediate_ca_key  = tls_private_key.intermediate_ca.private_key_pem
  
  # Compliance requirements
  enable_automatic_replication = var.environment == "prod"
  
  labels = local.common_tags
}

# =============================================================================
# ZERO TRUST NETWORK SECURITY
# =============================================================================

# Network Security Groups and Policies
module "network_security" {
  source = "./network_security"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Zero Trust configuration
  zero_trust_enabled = true
  micro_segmentation = true
  
  # Multi-cloud network IDs
  aws_vpc_id            = var.deploy_to_aws ? var.aws_vpc_id : null
  azure_vnet_id         = var.deploy_to_azure ? var.azure_vnet_id : null
  gcp_vpc_id           = var.deploy_to_gcp ? var.gcp_vpc_id : null
  
  # Network segmentation
  allowed_cidr_blocks = var.allowed_cidr_blocks
  private_subnet_cidrs = var.private_subnet_cidrs
  
  # Security policies
  enable_ddos_protection = var.environment == "prod"
  enable_waf             = true
  enable_network_acls    = true
  
  tags = local.common_tags
}

# Service Mesh Security (Istio)
module "service_mesh" {
  source = "./service_mesh"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Istio configuration
  enable_mtls           = true
  enable_authorization  = true
  enable_telemetry     = true
  
  # Certificate management
  intermediate_ca_cert = tls_locally_signed_cert.intermediate_ca.cert_pem
  intermediate_ca_key  = tls_private_key.intermediate_ca.private_key_pem
  
  # Zero Trust policies
  default_deny_policy = true
  workload_identity   = true
  
  labels = local.common_tags
}

# =============================================================================
# IDENTITY AND ACCESS MANAGEMENT (IAM)
# =============================================================================

# Multi-cloud IAM integration
module "iam_management" {
  source = "./iam"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Zero Trust IAM
  enable_just_in_time_access = true
  enable_privileged_access   = true
  enable_conditional_access  = true
  
  # RBAC configuration
  enable_rbac                = true
  enable_attribute_based_access = true
  
  # Multi-cloud IAM
  deploy_to_aws   = var.deploy_to_aws
  deploy_to_azure = var.deploy_to_azure
  deploy_to_gcp   = var.deploy_to_gcp
  
  # Compliance requirements
  compliance_frameworks = var.compliance_frameworks
  
  tags = local.common_tags
}

# =============================================================================
# SECURITY SCANNING AND VULNERABILITY MANAGEMENT
# =============================================================================

# Container Security Scanning
module "container_security" {
  count  = local.security_scanning.container_scanning ? 1 : 0
  source = "./container_security"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Scanning configuration
  enable_runtime_protection = true
  enable_admission_control  = true
  enable_policy_enforcement = true
  
  # Vulnerability management
  scan_schedule          = var.security_scan_schedule
  vulnerability_threshold = "HIGH"
  
  # Compliance scanning
  compliance_benchmarks = var.compliance_frameworks
  
  tags = local.common_tags
}

# Infrastructure Security Scanning
module "infrastructure_security" {
  count  = local.security_scanning.infrastructure_scanning ? 1 : 0
  source = "./infrastructure_security"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Infrastructure scanning
  enable_config_assessment = true
  enable_compliance_checks = true
  enable_drift_detection   = true
  
  # Multi-cloud scanning
  deploy_to_aws   = var.deploy_to_aws
  deploy_to_azure = var.deploy_to_azure
  deploy_to_gcp   = var.deploy_to_gcp
  
  # Compliance frameworks
  compliance_frameworks = var.compliance_frameworks
  scan_schedule        = var.security_scan_schedule
  
  tags = local.common_tags
}

# =============================================================================
# DATA PROTECTION AND ENCRYPTION
# =============================================================================

# Data Encryption Management
module "data_encryption" {
  source = "./data_encryption"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Encryption configuration
  enable_encryption_at_rest   = var.enable_encryption
  enable_encryption_in_transit = var.enable_encryption
  enable_field_level_encryption = contains(var.compliance_frameworks, "HIPAA")
  
  # Key management
  key_rotation_enabled = true
  key_rotation_days    = 90
  
  # Compliance requirements
  compliance_frameworks = var.compliance_frameworks
  
  # Multi-cloud KMS
  deploy_to_aws   = var.deploy_to_aws
  deploy_to_azure = var.deploy_to_azure
  deploy_to_gcp   = var.deploy_to_gcp
  
  tags = local.common_tags
}

# =============================================================================
# COMPLIANCE AND AUDIT LOGGING
# =============================================================================

# Centralized Audit Logging
module "audit_logging" {
  count  = var.enable_audit_logs ? 1 : 0
  source = "./audit_logging"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Audit configuration
  enable_real_time_monitoring = true
  enable_compliance_reporting = true
  enable_threat_detection     = true
  
  # Log retention based on compliance
  log_retention_days = contains(var.compliance_frameworks, "SOC2") ? 2555 : 365
  
  # Multi-cloud logging
  deploy_to_aws   = var.deploy_to_aws
  deploy_to_azure = var.deploy_to_azure
  deploy_to_gcp   = var.deploy_to_gcp
  
  # Compliance frameworks
  compliance_frameworks = var.compliance_frameworks
  
  tags = local.common_tags
}

# Security Information and Event Management (SIEM)
module "siem" {
  count  = var.environment == "prod" ? 1 : 0
  source = "./siem"
  
  project_name = var.project_name
  environment  = var.environment
  
  # SIEM configuration
  enable_correlation_rules = true
  enable_threat_intelligence = true
  enable_automated_response = true
  
  # Integration with audit logging
  audit_log_sources = var.enable_audit_logs ? [
    module.audit_logging[0].cloudwatch_log_group,
    module.audit_logging[0].azure_log_analytics,
    module.audit_logging[0].stackdriver_logging
  ] : []
  
  tags = local.common_tags
}

# =============================================================================
# INCIDENT RESPONSE AND FORENSICS
# =============================================================================

# Incident Response Automation
module "incident_response" {
  source = "./incident_response"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Incident response configuration
  enable_automated_containment = true
  enable_forensic_collection   = true
  enable_notification_system   = true
  
  # Response procedures
  security_contact_email = var.security_contact_email
  escalation_webhook    = var.escalation_webhook_url
  
  # Compliance requirements
  breach_notification_required = contains(var.compliance_frameworks, "GDPR")
  
  tags = local.common_tags
}

# =============================================================================
# SECURITY MONITORING AND ALERTING
# =============================================================================

# Security Monitoring Dashboard
module "security_monitoring" {
  source = "./security_monitoring"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Monitoring configuration
  enable_real_time_alerts    = true
  enable_anomaly_detection   = true
  enable_behavioral_analysis = true
  
  # Alert destinations
  slack_webhook_url    = var.slack_webhook_url
  email_notifications  = var.security_notifications_email
  
  # Integration with other security modules
  siem_enabled = var.environment == "prod"
  
  tags = local.common_tags
}

# =============================================================================
# BACKUP AND DISASTER RECOVERY SECURITY
# =============================================================================

# Secure Backup Management
module "backup_security" {
  source = "./backup_security"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Backup security
  enable_backup_encryption = true
  enable_immutable_backups = var.environment == "prod"
  enable_cross_region_replication = var.enable_disaster_recovery
  
  # Compliance requirements
  backup_retention_years = contains(var.compliance_frameworks, "SOC2") ? 7 : 1
  
  # Multi-cloud backup
  deploy_to_aws   = var.deploy_to_aws
  deploy_to_azure = var.deploy_to_azure
  deploy_to_gcp   = var.deploy_to_gcp
  
  tags = local.common_tags
}

# =============================================================================
# PRIVACY AND DATA GOVERNANCE
# =============================================================================

# Data Privacy Controls
module "data_privacy" {
  count  = contains(var.compliance_frameworks, "GDPR") ? 1 : 0
  source = "./data_privacy"
  
  project_name = var.project_name
  environment  = var.environment
  
  # Privacy configuration
  enable_data_classification = true
  enable_consent_management  = true
  enable_data_anonymization  = true
  enable_right_to_erasure    = true
  
  # Data discovery and cataloging
  enable_sensitive_data_discovery = true
  enable_data_lineage_tracking   = var.enable_data_lineage
  
  tags = local.common_tags
}

# =============================================================================
# THIRD-PARTY SECURITY INTEGRATIONS
# =============================================================================

# Security Orchestration, Automation and Response (SOAR)
module "soar_integration" {
  count  = var.environment == "prod" ? 1 : 0
  source = "./soar"
  
  project_name = var.project_name
  environment  = var.environment
  
  # SOAR configuration
  enable_automated_playbooks = true
  enable_threat_hunting      = true
  enable_case_management     = true
  
  # Integration endpoints
  security_tools = {
    siem_endpoint     = var.environment == "prod" ? module.siem[0].endpoint : null
    scanner_endpoints = local.security_scanning.container_scanning ? [
      module.container_security[0].scanner_endpoint
    ] : []
  }
  
  tags = local.common_tags
}

# =============================================================================
# OUTPUTS
# =============================================================================

# Security endpoints and configurations
output "security_endpoints" {
  description = "Security service endpoints"
  value = {
    ca_certificate = tls_self_signed_cert.root_ca.cert_pem
    intermediate_ca = tls_locally_signed_cert.intermediate_ca.cert_pem
    
    secrets_management = {
      aws_secrets_arn    = var.deploy_to_aws ? module.aws_secrets[0].secrets_manager_arn : null
      azure_keyvault_url = var.deploy_to_azure ? module.azure_secrets[0].key_vault_uri : null
      gcp_secret_id      = var.deploy_to_gcp ? module.gcp_secrets[0].secret_manager_id : null
    }
    
    security_monitoring = {
      dashboard_url = module.security_monitoring.dashboard_url
      alert_manager = module.security_monitoring.alert_manager_endpoint
    }
    
    audit_logging = var.enable_audit_logs ? {
      aws_cloudwatch    = var.deploy_to_aws ? module.audit_logging[0].cloudwatch_log_group : null
      azure_log_analytics = var.deploy_to_azure ? module.audit_logging[0].log_analytics_workspace : null
      gcp_cloud_logging = var.deploy_to_gcp ? module.audit_logging[0].cloud_logging_sink : null
    } : null
  }
  sensitive = true
}

# Compliance status
output "compliance_status" {
  description = "Compliance framework implementation status"
  value = {
    frameworks_enabled = var.compliance_frameworks
    soc2_controls = contains(var.compliance_frameworks, "SOC2") ? {
      encryption_enabled = var.enable_encryption
      audit_logging     = var.enable_audit_logs
      vulnerability_scanning = var.enable_security_scanning
    } : null
    
    gdpr_controls = contains(var.compliance_frameworks, "GDPR") ? {
      data_privacy_enabled = true
      consent_management   = true
      breach_notification  = true
    } : null
    
    hipaa_controls = contains(var.compliance_frameworks, "HIPAA") ? {
      phi_encryption = var.enable_encryption
      access_controls = true
      audit_controls = var.enable_audit_logs
    } : null
  }
}

# Security policies
output "security_policies" {
  description = "Applied security policies"
  value = {
    zero_trust_enabled = local.zero_trust_policies
    network_segmentation = module.network_security.segmentation_policies
    encryption_policies = module.data_encryption.encryption_status
  }
}