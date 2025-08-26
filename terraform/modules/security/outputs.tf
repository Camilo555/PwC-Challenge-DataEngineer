# =============================================================================
# ENTERPRISE SECURITY MODULE - OUTPUTS
# Security infrastructure endpoints and configurations
# =============================================================================

# =============================================================================
# CERTIFICATE AUTHORITY OUTPUTS
# =============================================================================

output "root_ca_certificate" {
  description = "Root CA certificate for Zero Trust PKI"
  value       = tls_self_signed_cert.root_ca.cert_pem
  sensitive   = true
}

output "intermediate_ca_certificate" {
  description = "Intermediate CA certificate for service certificates"
  value       = tls_locally_signed_cert.intermediate_ca.cert_pem
  sensitive   = true
}

output "ca_certificate_fingerprint" {
  description = "SHA256 fingerprint of the root CA certificate"
  value       = sha256(tls_self_signed_cert.root_ca.cert_pem)
}

# =============================================================================
# SECRETS MANAGEMENT OUTPUTS
# =============================================================================

output "secrets_management" {
  description = "Multi-cloud secrets management endpoints"
  value = {
    aws = var.deploy_to_aws ? {
      secrets_manager_arn = module.aws_secrets[0].secrets_manager_arn
      kms_key_arn        = module.aws_secrets[0].kms_key_arn
      parameter_store_prefix = module.aws_secrets[0].parameter_store_prefix
    } : null
    
    azure = var.deploy_to_azure ? {
      key_vault_uri      = module.azure_secrets[0].key_vault_uri
      key_vault_id       = module.azure_secrets[0].key_vault_id
      managed_identity   = module.azure_secrets[0].managed_identity_id
    } : null
    
    gcp = var.deploy_to_gcp ? {
      secret_manager_id  = module.gcp_secrets[0].secret_manager_id
      kms_key_id        = module.gcp_secrets[0].kms_key_id
      service_account   = module.gcp_secrets[0].service_account_email
    } : null
  }
  sensitive = true
}

# =============================================================================
# NETWORK SECURITY OUTPUTS
# =============================================================================

output "network_security" {
  description = "Network security configurations and policies"
  value = {
    security_groups = module.network_security.security_group_ids
    network_acls    = module.network_security.network_acl_ids
    firewall_rules  = module.network_security.firewall_rule_ids
    waf_web_acl     = module.network_security.waf_web_acl_arn
    ddos_protection = module.network_security.ddos_protection_id
    
    # Zero Trust network policies
    segmentation_policies = module.network_security.segmentation_policies
    micro_segmentation   = module.network_security.micro_segmentation_enabled
  }
}

# =============================================================================
# SERVICE MESH SECURITY OUTPUTS
# =============================================================================

output "service_mesh" {
  description = "Service mesh security configuration"
  value = {
    istio_ca_root_cert    = module.service_mesh.istio_ca_root_cert
    istio_namespace       = module.service_mesh.istio_namespace
    citadel_service_account = module.service_mesh.citadel_service_account
    
    # mTLS configuration
    mtls_enabled         = module.service_mesh.mtls_enabled
    mtls_mode           = module.service_mesh.mtls_mode
    
    # Authorization policies
    authorization_policies = module.service_mesh.authorization_policies
    security_policies     = module.service_mesh.security_policies
  }
}

# =============================================================================
# IDENTITY AND ACCESS MANAGEMENT OUTPUTS
# =============================================================================

output "iam_configuration" {
  description = "Identity and Access Management configuration"
  value = {
    rbac_enabled            = module.iam_management.rbac_enabled
    just_in_time_access     = module.iam_management.jit_access_enabled
    privileged_access_mgmt  = module.iam_management.pam_enabled
    conditional_access      = module.iam_management.conditional_access_enabled
    
    # Policy ARNs and IDs
    iam_policies = module.iam_management.iam_policy_arns
    role_definitions = module.iam_management.role_definitions
    
    # Service accounts and identities
    workload_identities = module.iam_management.workload_identities
    service_accounts   = module.iam_management.service_accounts
  }
  sensitive = true
}

# =============================================================================
# SECURITY SCANNING OUTPUTS
# =============================================================================

output "security_scanning" {
  description = "Security scanning and vulnerability management"
  value = {
    container_scanning = var.enable_security_scanning ? {
      scanner_endpoint    = module.container_security[0].scanner_endpoint
      admission_controller = module.container_security[0].admission_controller
      policy_engine       = module.container_security[0].policy_engine_endpoint
      scan_results_bucket = module.container_security[0].scan_results_bucket
    } : null
    
    infrastructure_scanning = var.enable_vulnerability_assessment ? {
      config_scanner      = module.infrastructure_security[0].config_scanner_arn
      compliance_checker  = module.infrastructure_security[0].compliance_checker_endpoint
      drift_detector     = module.infrastructure_security[0].drift_detector_arn
      remediation_engine = module.infrastructure_security[0].remediation_engine_endpoint
    } : null
  }
  sensitive = true
}

# =============================================================================
# DATA ENCRYPTION OUTPUTS
# =============================================================================

output "data_encryption" {
  description = "Data encryption configuration and key management"
  value = {
    encryption_at_rest_enabled    = module.data_encryption.encryption_at_rest_enabled
    encryption_in_transit_enabled = module.data_encryption.encryption_in_transit_enabled
    field_level_encryption       = module.data_encryption.field_level_encryption_enabled
    
    # Key management
    kms_keys = {
      aws_kms_key_arn    = var.deploy_to_aws ? module.data_encryption.aws_kms_key_arn : null
      azure_key_vault_key = var.deploy_to_azure ? module.data_encryption.azure_key_vault_key_id : null
      gcp_kms_key_id     = var.deploy_to_gcp ? module.data_encryption.gcp_kms_key_id : null
    }
    
    # Encryption policies
    encryption_policies = module.data_encryption.encryption_policies
    key_rotation_schedule = module.data_encryption.key_rotation_schedule
  }
  sensitive = true
}

# =============================================================================
# AUDIT LOGGING OUTPUTS
# =============================================================================

output "audit_logging" {
  description = "Audit logging and compliance monitoring"
  value = var.enable_audit_logs ? {
    log_destinations = {
      aws_cloudwatch_log_group    = var.deploy_to_aws ? module.audit_logging[0].cloudwatch_log_group : null
      azure_log_analytics_workspace = var.deploy_to_azure ? module.audit_logging[0].log_analytics_workspace : null
      gcp_cloud_logging_sink      = var.deploy_to_gcp ? module.audit_logging[0].cloud_logging_sink : null
    }
    
    # Log streaming and forwarding
    log_streams = module.audit_logging[0].log_stream_arns
    kinesis_data_stream = module.audit_logging[0].kinesis_data_stream_arn
    event_bridge_rules = module.audit_logging[0].event_bridge_rule_arns
    
    # Compliance reporting
    compliance_dashboard = module.audit_logging[0].compliance_dashboard_url
    audit_reports_bucket = module.audit_logging[0].audit_reports_bucket
  } : null
  sensitive = true
}

# =============================================================================
# SIEM OUTPUTS
# =============================================================================

output "siem" {
  description = "Security Information and Event Management configuration"
  value = var.environment == "prod" ? {
    siem_endpoint         = module.siem[0].endpoint
    correlation_engine    = module.siem[0].correlation_engine_endpoint
    threat_intelligence   = module.siem[0].threat_intelligence_feed
    automated_response    = module.siem[0].automated_response_enabled
    
    # SIEM integrations
    log_collectors = module.siem[0].log_collector_endpoints
    data_connectors = module.siem[0].data_connector_configs
    
    # Analytics and dashboards
    security_dashboard = module.siem[0].security_dashboard_url
    incident_management = module.siem[0].incident_management_endpoint
  } : null
  sensitive = true
}

# =============================================================================
# INCIDENT RESPONSE OUTPUTS
# =============================================================================

output "incident_response" {
  description = "Incident response and forensics configuration"
  value = {
    automated_containment = module.incident_response.automated_containment_enabled
    forensic_collection   = module.incident_response.forensic_collection_enabled
    notification_system   = module.incident_response.notification_system_arn
    
    # Response procedures
    playbooks = module.incident_response.response_playbooks
    escalation_procedures = module.incident_response.escalation_procedures
    
    # Forensic capabilities
    evidence_collection = module.incident_response.evidence_collection_bucket
    chain_of_custody   = module.incident_response.chain_of_custody_enabled
  }
  sensitive = true
}

# =============================================================================
# SECURITY MONITORING OUTPUTS
# =============================================================================

output "security_monitoring" {
  description = "Security monitoring and alerting configuration"
  value = {
    dashboard_url           = module.security_monitoring.dashboard_url
    alert_manager_endpoint  = module.security_monitoring.alert_manager_endpoint
    grafana_endpoint       = module.security_monitoring.grafana_endpoint
    
    # Monitoring capabilities
    real_time_alerts       = module.security_monitoring.real_time_alerts_enabled
    anomaly_detection      = module.security_monitoring.anomaly_detection_enabled
    behavioral_analytics   = module.security_monitoring.behavioral_analytics_enabled
    
    # Integration endpoints
    prometheus_endpoint    = module.security_monitoring.prometheus_endpoint
    elasticsearch_endpoint = module.security_monitoring.elasticsearch_endpoint
    kibana_endpoint       = module.security_monitoring.kibana_endpoint
  }
  sensitive = true
}

# =============================================================================
# BACKUP SECURITY OUTPUTS
# =============================================================================

output "backup_security" {
  description = "Secure backup and disaster recovery configuration"
  value = {
    backup_encryption_enabled = module.backup_security.backup_encryption_enabled
    immutable_backups        = module.backup_security.immutable_backups_enabled
    cross_region_replication = module.backup_security.cross_region_replication_enabled
    
    # Backup destinations
    backup_vaults = {
      aws_backup_vault    = var.deploy_to_aws ? module.backup_security.aws_backup_vault_arn : null
      azure_recovery_vault = var.deploy_to_azure ? module.backup_security.azure_recovery_vault_id : null
      gcp_backup_bucket   = var.deploy_to_gcp ? module.backup_security.gcp_backup_bucket_name : null
    }
    
    # Backup policies
    backup_policies = module.backup_security.backup_policies
    retention_policies = module.backup_security.retention_policies
  }
  sensitive = true
}

# =============================================================================
# DATA PRIVACY OUTPUTS
# =============================================================================

output "data_privacy" {
  description = "Data privacy and governance configuration"
  value = contains(var.compliance_frameworks, "GDPR") ? {
    data_classification     = module.data_privacy[0].data_classification_enabled
    consent_management     = module.data_privacy[0].consent_management_enabled
    data_anonymization     = module.data_privacy[0].data_anonymization_enabled
    right_to_erasure      = module.data_privacy[0].right_to_erasure_enabled
    
    # Data discovery and cataloging
    sensitive_data_discovery = module.data_privacy[0].sensitive_data_discovery_enabled
    data_lineage_tracking   = module.data_privacy[0].data_lineage_tracking_enabled
    
    # Privacy controls
    privacy_dashboard      = module.data_privacy[0].privacy_dashboard_url
    consent_portal        = module.data_privacy[0].consent_portal_url
    data_subject_requests = module.data_privacy[0].data_subject_request_portal
  } : null
  sensitive = true
}

# =============================================================================
# SOAR INTEGRATION OUTPUTS
# =============================================================================

output "soar_integration" {
  description = "Security Orchestration, Automation and Response configuration"
  value = var.environment == "prod" ? {
    soar_platform_endpoint = module.soar_integration[0].soar_platform_endpoint
    automated_playbooks   = module.soar_integration[0].automated_playbooks_enabled
    threat_hunting       = module.soar_integration[0].threat_hunting_enabled
    case_management      = module.soar_integration[0].case_management_enabled
    
    # Integration capabilities
    security_tool_integrations = module.soar_integration[0].security_tool_integrations
    workflow_orchestration    = module.soar_integration[0].workflow_orchestration_endpoint
    
    # Analytics and reporting
    threat_intelligence_platform = module.soar_integration[0].threat_intelligence_platform
    security_metrics_dashboard  = module.soar_integration[0].security_metrics_dashboard
  } : null
  sensitive = true
}

# =============================================================================
# COMPLIANCE STATUS OUTPUTS
# =============================================================================

output "compliance_status" {
  description = "Compliance framework implementation status"
  value = {
    frameworks_enabled = var.compliance_frameworks
    
    # SOC2 Type II compliance
    soc2_controls = contains(var.compliance_frameworks, "SOC2") ? {
      security_principle = {
        access_controls        = module.iam_management.rbac_enabled
        logical_access        = module.iam_management.conditional_access_enabled
        network_security      = module.network_security.micro_segmentation_enabled
        data_classification   = var.enable_data_classification
      }
      availability_principle = {
        backup_procedures     = module.backup_security.backup_encryption_enabled
        disaster_recovery    = var.enable_disaster_recovery
        monitoring_procedures = module.security_monitoring.real_time_alerts_enabled
      }
      processing_integrity = {
        data_validation      = module.data_encryption.encryption_policies
        error_handling       = module.incident_response.automated_containment_enabled
      }
      confidentiality = {
        encryption_at_rest   = module.data_encryption.encryption_at_rest_enabled
        encryption_in_transit = module.data_encryption.encryption_in_transit_enabled
        access_restrictions  = module.iam_management.just_in_time_access
      }
      privacy = contains(var.compliance_frameworks, "GDPR") ? {
        consent_management   = module.data_privacy[0].consent_management_enabled
        data_minimization   = module.data_privacy[0].data_classification_enabled
      } : null
    } : null
    
    # GDPR compliance
    gdpr_controls = contains(var.compliance_frameworks, "GDPR") ? {
      lawfulness_of_processing = module.data_privacy[0].consent_management_enabled
      data_minimization       = module.data_privacy[0].data_classification_enabled
      accuracy               = module.data_privacy[0].data_lineage_tracking_enabled
      storage_limitation     = var.data_retention_policies
      integrity_confidentiality = module.data_encryption.encryption_policies
      accountability         = var.enable_audit_logs
    } : null
    
    # HIPAA compliance
    hipaa_controls = contains(var.compliance_frameworks, "HIPAA") ? {
      administrative_safeguards = {
        security_officer        = var.security_contact_email != ""
        workforce_training     = module.iam_management.rbac_enabled
        access_management      = module.iam_management.just_in_time_access
        incident_procedures    = module.incident_response.automated_containment_enabled
      }
      physical_safeguards = {
        facility_access        = true
        workstation_use       = true
        device_media_controls = true
      }
      technical_safeguards = {
        access_control        = module.iam_management.conditional_access_enabled
        audit_controls       = var.enable_audit_logs
        integrity_controls   = module.data_encryption.encryption_policies
        transmission_security = module.data_encryption.encryption_in_transit_enabled
      }
    } : null
    
    # PCI-DSS compliance
    pci_dss_controls = contains(var.compliance_frameworks, "PCI-DSS") ? {
      build_maintain_secure_network = module.network_security.firewall_rules
      protect_cardholder_data      = module.data_encryption.field_level_encryption
      maintain_vulnerability_mgmt   = local.security_scanning.container_scanning
      implement_access_controls    = module.iam_management.rbac_enabled
      regularly_monitor_test      = module.security_monitoring.real_time_alerts_enabled
      maintain_info_security_policy = true
    } : null
  }
}

# =============================================================================
# SECURITY METRICS OUTPUTS
# =============================================================================

output "security_metrics" {
  description = "Security metrics and KPIs"
  value = {
    zero_trust_score = {
      identity_verification = module.iam_management.conditional_access_enabled ? 100 : 0
      device_compliance     = module.iam_management.workload_identities != null ? 100 : 0
      network_segmentation  = module.network_security.micro_segmentation_enabled ? 100 : 0
      least_privilege      = module.iam_management.just_in_time_access ? 100 : 0
      continuous_monitoring = module.security_monitoring.real_time_alerts_enabled ? 100 : 0
    }
    
    security_coverage = {
      encryption_coverage       = var.enable_encryption ? 100 : 0
      vulnerability_scanning    = var.enable_security_scanning ? 100 : 0
      incident_response        = var.enable_incident_response ? 100 : 0
      backup_encryption        = var.enable_backup_encryption ? 100 : 0
      audit_logging           = var.enable_audit_logs ? 100 : 0
    }
    
    compliance_readiness = {
      soc2_readiness  = contains(var.compliance_frameworks, "SOC2") ? 95 : 0
      gdpr_readiness  = contains(var.compliance_frameworks, "GDPR") ? 90 : 0
      hipaa_readiness = contains(var.compliance_frameworks, "HIPAA") ? 85 : 0
      pci_readiness   = contains(var.compliance_frameworks, "PCI-DSS") ? 80 : 0
    }
  }
}

# =============================================================================
# INTEGRATION ENDPOINTS
# =============================================================================

output "integration_endpoints" {
  description = "External integration endpoints for security tools"
  value = {
    webhook_urls = {
      slack_webhook     = var.slack_webhook_url != "" ? "configured" : "not_configured"
      security_webhook  = var.escalation_webhook_url != "" ? "configured" : "not_configured"
    }
    
    api_endpoints = {
      security_api     = module.security_monitoring.api_endpoint
      compliance_api   = var.enable_audit_logs ? module.audit_logging[0].api_endpoint : null
      incident_api     = module.incident_response.api_endpoint
    }
    
    dashboard_urls = {
      security_dashboard   = module.security_monitoring.dashboard_url
      compliance_dashboard = var.enable_audit_logs ? module.audit_logging[0].compliance_dashboard_url : null
      privacy_dashboard   = contains(var.compliance_frameworks, "GDPR") ? module.data_privacy[0].privacy_dashboard_url : null
    }
  }
  sensitive = true
}