# Multi-Cloud Disaster Recovery Module
# Implements comprehensive DR strategy across cloud providers

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
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  # DR strategy based on RTO/RPO requirements
  dr_strategy = var.rto_minutes <= 30 ? "active-active" : var.rto_minutes <= 120 ? "active-passive" : "backup-restore"
  
  # Replication configuration
  replication_config = {
    enabled = var.enable_cross_cloud_replication
    primary_cloud = var.primary_cloud
    secondary_cloud = var.secondary_cloud
    strategy = local.dr_strategy
  }
  
  # Recovery objectives
  recovery_objectives = {
    rto_minutes = var.rto_minutes
    rpo_minutes = var.rpo_minutes
    tier = var.rto_minutes <= 30 ? "tier1" : var.rto_minutes <= 240 ? "tier2" : "tier3"
  }
}

# ============================================================================
# AWS DISASTER RECOVERY
# ============================================================================

# AWS Backup Vault
resource "aws_backup_vault" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name        = "${local.name_prefix}-backup-vault"
  kms_key_arn = aws_kms_key.backup[0].arn
  
  tags = var.tags
}

# AWS Backup Plan
resource "aws_backup_plan" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-backup-plan"
  
  # Critical data - frequent backups
  rule {
    rule_name         = "critical_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 */4 * * ? *)"  # Every 4 hours
    
    start_window      = 60
    completion_window = 300
    
    lifecycle {
      cold_storage_after = 7
      delete_after      = var.backup_retention_days
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "critical"
      RPO        = "${var.rpo_minutes}m"
    })
  }
  
  # Regular data - daily backups
  rule {
    rule_name         = "regular_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 2 * * ? *)"  # Daily at 2 AM
    
    lifecycle {
      cold_storage_after = 30
      delete_after      = var.backup_retention_days
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "regular"
    })
  }
  
  # Archive data - weekly backups
  rule {
    rule_name         = "archive_data_backup"
    target_vault_name = aws_backup_vault.main[0].name
    schedule          = "cron(0 1 ? * SUN *)"  # Weekly on Sunday
    
    lifecycle {
      cold_storage_after = 1
      delete_after      = 2555  # 7 years
    }
    
    recovery_point_tags = merge(var.tags, {
      BackupType = "archive"
    })
  }
  
  tags = var.tags
}

# AWS Backup Selection
resource "aws_backup_selection" "main" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  iam_role_arn = aws_iam_role.backup[0].arn
  name         = "${local.name_prefix}-backup-selection"
  plan_id      = aws_backup_plan.main[0].id
  
  selection_tag {
    type  = "STRINGEQUALS"
    key   = "Environment"
    value = var.environment
  }
  
  selection_tag {
    type  = "STRINGEQUALS"
    key   = "BackupRequired"
    value = "true"
  }
}

# KMS Key for AWS Backup
resource "aws_kms_key" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  description = "KMS key for AWS Backup encryption"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current[0].account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow AWS Backup to use the key"
        Effect = "Allow"
        Principal = {
          Service = "backup.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:CreateGrant"
        ]
        Resource = "*"
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_kms_alias" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name          = "alias/${local.name_prefix}-backup"
  target_key_id = aws_kms_key.backup[0].key_id
}

# IAM Role for AWS Backup
resource "aws_iam_role" "backup" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-backup-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "backup.amazonaws.com"
      }
    }]
  })
  
  managed_policy_arns = [
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForBackup",
    "arn:aws:iam::aws:policy/service-role/AWSBackupServiceRolePolicyForRestores"
  ]
  
  tags = var.tags
}

# AWS Lambda for DR Orchestration
resource "aws_lambda_function" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  filename         = data.archive_file.dr_orchestrator_zip[0].output_path
  function_name    = "${local.name_prefix}-dr-orchestrator"
  role            = aws_iam_role.dr_orchestrator[0].arn
  handler         = "index.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900  # 15 minutes
  
  environment {
    variables = {
      PRIMARY_CLOUD     = var.primary_cloud
      SECONDARY_CLOUD   = var.secondary_cloud
      RTO_MINUTES      = var.rto_minutes
      RPO_MINUTES      = var.rpo_minutes
      DR_STRATEGY      = local.dr_strategy
      DATABASE_PRIMARY = var.database_endpoints.primary
      DATABASE_SECONDARY = var.database_endpoints.secondary
    }
  }
  
  dead_letter_config {
    target_arn = aws_sqs_queue.dr_dlq[0].arn
  }
  
  tags = var.tags
}

# Lambda function code for DR orchestration
data "archive_file" "dr_orchestrator_zip" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/dr_orchestrator.zip"
  
  source {
    content = templatefile("${path.module}/templates/dr_orchestrator.py.tpl", {
      primary_cloud   = var.primary_cloud
      secondary_cloud = var.secondary_cloud
      rto_minutes    = var.rto_minutes
    })
    filename = "index.py"
  }
}

# IAM Role for DR Orchestrator Lambda
resource "aws_iam_role" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-orchestrator-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
  
  tags = var.tags
}

# IAM Policy for DR Orchestrator
resource "aws_iam_role_policy" "dr_orchestrator" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-orchestrator-policy"
  role = aws_iam_role.dr_orchestrator[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "backup:StartRestoreJob",
          "backup:DescribeRestoreJob",
          "backup:ListBackupJobs",
          "backup:ListRestoreJobs",
          "rds:CreateDBInstanceReadReplica",
          "rds:PromoteReadReplica",
          "rds:DescribeDBInstances",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:GetBucketReplication",
          "ec2:DescribeInstances",
          "ec2:StartInstances",
          "ec2:StopInstances",
          "route53:ChangeResourceRecordSets",
          "route53:GetHostedZone",
          "sns:Publish",
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage"
        ]
        Resource = "*"
      }
    ]
  })
}

# SQS Dead Letter Queue for DR operations
resource "aws_sqs_queue" "dr_dlq" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-dlq"
  
  message_retention_seconds = 1209600  # 14 days
  
  tags = var.tags
}

# ============================================================================
# AZURE DISASTER RECOVERY
# ============================================================================

# Azure Site Recovery Vault
resource "azurerm_recovery_services_vault" "main" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-recovery-vault"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku                 = "Standard"
  
  storage_mode_type = "GeoRedundant"
  cross_region_restore_enabled = true
  
  tags = var.tags
}

# Azure Backup Policy for VMs
resource "azurerm_backup_policy_vm" "main" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-vm-backup-policy"
  resource_group_name = var.azure_resource_group_name
  recovery_vault_name = azurerm_recovery_services_vault.main[0].name
  
  backup {
    frequency = "Daily"
    time      = "02:00"
  }
  
  retention_daily {
    count = var.backup_retention_days
  }
  
  retention_weekly {
    count    = 52  # 1 year
    weekdays = ["Sunday"]
  }
  
  retention_monthly {
    count    = 84  # 7 years
    weekdays = ["Sunday"]
    weeks    = ["First"]
  }
  
  tags = var.tags
}

# Azure Automation Account for DR
resource "azurerm_automation_account" "dr_automation" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${local.name_prefix}-dr-automation"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  sku_name           = "Basic"
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# Azure Runbook for DR Failover
resource "azurerm_automation_runbook" "dr_failover" {
  count = var.primary_cloud == "azure" || var.secondary_cloud == "azure" ? 1 : 0
  
  name                    = "${local.name_prefix}-dr-failover"
  location               = var.azure_location
  resource_group_name    = var.azure_resource_group_name
  automation_account_name = azurerm_automation_account.dr_automation[0].name
  log_verbose            = "true"
  log_progress           = "true"
  description           = "DR failover orchestration runbook"
  runbook_type          = "PowerShell"
  
  content = templatefile("${path.module}/templates/azure_dr_failover.ps1.tpl", {
    primary_cloud        = var.primary_cloud
    secondary_cloud      = var.secondary_cloud
    resource_group_name  = var.azure_resource_group_name
    recovery_vault_name  = azurerm_recovery_services_vault.main[0].name
  })
  
  tags = var.tags
}

# ============================================================================
# GCP DISASTER RECOVERY
# ============================================================================

# GCP Backup and DR Service
resource "google_compute_region_backend_service" "dr_backend" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name                  = "${local.name_prefix}-dr-backend"
  region                = var.gcp_region
  protocol              = "HTTP"
  timeout_sec           = 30
  load_balancing_scheme = "EXTERNAL"
  
  health_checks = [google_compute_health_check.dr_health_check[0].id]
  
  failover_policy {
    disable_connection_drain_on_failover = false
    drop_traffic_if_unhealthy           = true
    failover_ratio                      = 1.0
  }
}

# GCP Health Check for DR
resource "google_compute_health_check" "dr_health_check" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name               = "${local.name_prefix}-dr-health-check"
  check_interval_sec = 10
  timeout_sec        = 5
  
  http_health_check {
    port               = 80
    request_path       = "/health"
    proxy_header       = "NONE"
  }
}

# GCP Cloud Function for DR Orchestration
resource "google_cloudfunctions_function" "dr_orchestrator" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name        = "${local.name_prefix}-dr-orchestrator"
  description = "DR orchestration function"
  runtime     = "python39"
  
  available_memory_mb   = 512
  timeout              = 540
  source_archive_bucket = google_storage_bucket.dr_functions[0].name
  source_archive_object = google_storage_bucket_object.dr_orchestrator_source[0].name
  
  trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.dr_events[0].name
  }
  
  entry_point = "orchestrate_dr"
  
  environment_variables = {
    PRIMARY_CLOUD     = var.primary_cloud
    SECONDARY_CLOUD   = var.secondary_cloud
    RTO_MINUTES      = var.rto_minutes
    RPO_MINUTES      = var.rpo_minutes
    GCP_PROJECT      = var.gcp_project_id
  }
  
  labels = var.tags
}

# GCP Storage for Cloud Functions
resource "google_storage_bucket" "dr_functions" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name     = "${local.name_prefix}-dr-functions-${random_string.bucket_suffix.result}"
  location = var.gcp_region
  
  labels = var.tags
}

resource "google_storage_bucket_object" "dr_orchestrator_source" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name   = "dr-orchestrator-source.zip"
  bucket = google_storage_bucket.dr_functions[0].name
  source = data.archive_file.gcp_dr_orchestrator_zip[0].output_path
}

data "archive_file" "gcp_dr_orchestrator_zip" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  type        = "zip"
  output_path = "/tmp/gcp_dr_orchestrator.zip"
  
  source {
    content = templatefile("${path.module}/templates/gcp_dr_orchestrator.py.tpl", {
      project_id = var.gcp_project_id
    })
    filename = "main.py"
  }
  
  source {
    content = "google-cloud-compute==1.14.0\ngoogle-cloud-sql==3.8.0\ngoogle-cloud-storage==2.10.0"
    filename = "requirements.txt"
  }
}

# GCP Pub/Sub for DR Events
resource "google_pubsub_topic" "dr_events" {
  count = var.primary_cloud == "gcp" || var.secondary_cloud == "gcp" ? 1 : 0
  
  name = "${local.name_prefix}-dr-events"
  
  labels = var.tags
}

# ============================================================================
# KUBERNETES DISASTER RECOVERY
# ============================================================================

# Velero Backup Configuration
resource "kubernetes_namespace" "velero" {
  count = var.enable_k8s_backup ? 1 : 0
  
  metadata {
    name = "velero"
    
    labels = {
      app = "velero"
      purpose = "disaster-recovery"
    }
  }
}

# Velero Backup Schedule
resource "kubernetes_config_map" "velero_backup_schedule" {
  count = var.enable_k8s_backup ? 1 : 0
  
  metadata {
    name      = "velero-backup-schedule"
    namespace = kubernetes_namespace.velero[0].metadata[0].name
  }
  
  data = {
    "backup-schedule.yaml" = templatefile("${path.module}/templates/velero-schedule.yaml.tpl", {
      environment     = var.environment
      backup_schedule = "0 2 * * *"  # Daily at 2 AM
      ttl_hours      = var.backup_retention_days * 24
    })
  }
}

# ============================================================================
# DR MONITORING AND ALERTING
# ============================================================================

# CloudWatch Dashboard for DR Metrics (AWS)
resource "aws_cloudwatch_dashboard" "dr_dashboard" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  dashboard_name = "${local.name_prefix}-dr-dashboard"
  
  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        
        properties = {
          metrics = [
            ["AWS/Backup", "NumberOfBackupJobsCompleted"],
            ["AWS/Backup", "NumberOfBackupJobsFailed"],
            ["AWS/Lambda", "Duration", "FunctionName", "${local.name_prefix}-dr-orchestrator"],
            ["AWS/Lambda", "Errors", "FunctionName", "${local.name_prefix}-dr-orchestrator"]
          ]
          period = 300
          stat   = "Sum"
          region = data.aws_region.current[0].name
          title  = "DR Metrics"
        }
      }
    ]
  })
}

# SNS Topic for DR Alerts
resource "aws_sns_topic" "dr_alerts" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  name = "${local.name_prefix}-dr-alerts"
  
  tags = var.tags
}

# SNS Topic Subscription for DR Alerts
resource "aws_sns_topic_subscription" "dr_alerts_email" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" && var.alert_email != null ? 1 : 0
  
  topic_arn = aws_sns_topic.dr_alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# CloudWatch Alarm for Backup Failures
resource "aws_cloudwatch_metric_alarm" "backup_failures" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
  
  alarm_name          = "${local.name_prefix}-backup-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "NumberOfBackupJobsFailed"
  namespace           = "AWS/Backup"
  period              = "300"
  statistic           = "Sum"
  threshold           = "0"
  alarm_description   = "This metric monitors backup job failures"
  
  alarm_actions = [aws_sns_topic.dr_alerts[0].arn]
  
  tags = var.tags
}

# ============================================================================
# ADVANCED MULTI-REGION DATA REPLICATION
# ============================================================================

# Cross-region database replication for <15 minutes RPO
resource "kubernetes_config_map" "data_replication_config" {
  metadata {
    name      = "${local.name_prefix}-data-replication-config"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "disaster-recovery"
      tier      = "data-replication"
    })
  }
  
  data = {
    "replication-config" = jsonencode({
      rpo_target_minutes = var.rpo_minutes
      rto_target_minutes = var.rto_minutes
      
      database_replication = {
        enabled = true
        strategy = var.rpo_minutes <= 15 ? "synchronous" : "asynchronous"
        
        postgresql = {
          streaming_replication = true
          wal_level = "replica"
          archive_mode = "on"
          archive_command = "test ! -f /backup/archive/%f && cp %p /backup/archive/%f"
          hot_standby = "on"
          max_wal_senders = 10
          wal_keep_segments = 100
          checkpoint_timeout = "5min"
          checkpoint_completion_target = 0.9
        }
        
        cross_region_sync = {
          enabled = var.enable_cross_cloud_replication
          primary_regions = [var.primary_region]
          secondary_regions = [var.secondary_region, var.tertiary_region]
          sync_interval_seconds = var.rpo_minutes * 60
          
          conflict_resolution = "primary_wins"
          encryption_in_transit = true
          compression_enabled = true
        }
        
        backup_retention = {
          point_in_time_recovery_days = 35
          full_backup_retention_days = var.backup_retention_days
          incremental_backup_interval_minutes = 15
          transaction_log_backup_interval_minutes = 5
        }
      }
      
      object_storage_replication = {
        enabled = true
        cross_region = var.enable_cross_cloud_replication
        
        aws_s3 = {
          cross_region_replication = true
          versioning_enabled = true
          lifecycle_policies = [
            {
              name = "transition_to_ia"
              days = 30
              storage_class = "STANDARD_IA"
            },
            {
              name = "transition_to_glacier"
              days = 90
              storage_class = "GLACIER"
            }
          ]
        }
        
        azure_storage = {
          geo_redundant_storage = true
          replication_type = "GRS"
          access_tier = "Hot"
          
          lifecycle_management = {
            enabled = true
            rules = [
              {
                name = "move_to_cool"
                days_after_modification = 30
                tier = "Cool"
              },
              {
                name = "move_to_archive"
                days_after_modification = 90
                tier = "Archive"
              }
            ]
          }
        }
        
        gcp_storage = {
          regional_replication = true
          multi_regional = true
          storage_class = "STANDARD"
          
          lifecycle_conditions = [
            {
              action = "SetStorageClass"
              storage_class = "NEARLINE"
              age = 30
            },
            {
              action = "SetStorageClass"
              storage_class = "COLDLINE"
              age = 90
            }
          ]
        }
      }
      
      application_state_replication = {
        enabled = true
        kubernetes_resources = [
          "deployments", "services", "configmaps", "secrets", 
          "persistentvolumeclaims", "ingresses", "horizontalpodautoscalers"
        ]
        
        etcd_backup = {
          enabled = true
          schedule = "*/5 * * * *"  # Every 5 minutes
          retention_count = 24      # Keep 24 backups (2 hours)
          encryption = true
        }
        
        persistent_volume_snapshots = {
          enabled = true
          schedule = "*/15 * * * *"  # Every 15 minutes
          cross_zone = true
          retention_count = 96       # Keep 96 snapshots (24 hours)
        }
      }
    })
    
    "failover-automation" = jsonencode({
      automated_failover = {
        enabled = var.enable_automated_failover
        health_check_interval_seconds = 30
        failure_threshold = 3
        recovery_timeout_minutes = var.rto_minutes
        
        health_checks = [
          {
            name = "database_connectivity"
            endpoint = var.database_endpoints.primary
            timeout_seconds = 10
            expected_response_time_ms = 1000
          },
          {
            name = "api_availability"
            endpoint = var.api_health_endpoint
            timeout_seconds = 5
            expected_status_codes = [200, 204]
          },
          {
            name = "storage_accessibility"
            type = "storage_health"
            timeout_seconds = 15
          }
        ]
        
        failover_sequence = [
          {
            step = 1
            action = "stop_primary_traffic"
            timeout_minutes = 2
          },
          {
            step = 2
            action = "promote_secondary_database"
            timeout_minutes = 5
          },
          {
            step = 3
            action = "update_dns_records"
            timeout_minutes = 2
          },
          {
            step = 4
            action = "start_secondary_applications"
            timeout_minutes = 10
          },
          {
            step = 5
            action = "validate_secondary_health"
            timeout_minutes = 5
          },
          {
            step = 6
            action = "redirect_traffic"
            timeout_minutes = 1
          }
        ]
        
        notification_channels = [
          var.alert_email,
          var.slack_webhook_url,
          var.pagerduty_integration_key
        ]
      }
      
      failback_automation = {
        enabled = true
        manual_approval_required = var.environment == "prod"
        health_check_duration_minutes = 15
        
        failback_sequence = [
          {
            step = 1
            action = "validate_primary_health"
            timeout_minutes = 10
          },
          {
            step = 2
            action = "sync_data_from_secondary"
            timeout_minutes = 30
          },
          {
            step = 3
            action = "switch_traffic_gradually"
            timeout_minutes = 15
            traffic_percentage_steps = [10, 25, 50, 75, 100]
          },
          {
            step = 4
            action = "demote_secondary"
            timeout_minutes = 5
          }
        ]
      }
      
      disaster_scenarios = [
        {
          name = "primary_region_failure"
          triggers = ["region_outage", "network_partition"]
          failover_target = "secondary_region"
          estimated_rto_minutes = var.rto_minutes
        },
        {
          name = "database_corruption"
          triggers = ["data_corruption", "ransomware"]
          failover_target = "point_in_time_recovery"
          estimated_rto_minutes = var.rto_minutes * 1.5
        },
        {
          name = "application_failure"
          triggers = ["application_crash", "memory_leak"]
          failover_target = "restart_applications"
          estimated_rto_minutes = 5
        },
        {
          name = "storage_failure"
          triggers = ["disk_failure", "storage_corruption"]
          failover_target = "restore_from_backup"
          estimated_rto_minutes = var.rto_minutes * 2
        }
      ]
    })
  }
}

# Advanced disaster recovery orchestration deployment
resource "kubernetes_deployment" "dr_orchestrator" {
  metadata {
    name      = "${local.name_prefix}-dr-orchestrator"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "disaster-recovery"
      tier      = "orchestration"
    })
  }
  
  spec {
    replicas = var.environment == "prod" ? 3 : 2  # High availability for prod
    
    selector {
      match_labels = {
        app       = "${local.name_prefix}-dr-orchestrator"
        component = "disaster-recovery"
      }
    }
    
    template {
      metadata {
        labels = merge(var.tags, {
          app       = "${local.name_prefix}-dr-orchestrator"
          component = "disaster-recovery"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "9090"
          "prometheus.io/path"   = "/metrics"
        }
      }
      
      spec {
        service_account_name = var.service_account_name
        
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                label_selector {
                  match_expressions {
                    key      = "component"
                    operator = "In"
                    values   = ["disaster-recovery"]
                  }
                }
                topology_key = "kubernetes.io/hostname"
              }
            }
          }
        }
        
        container {
          name  = "dr-orchestrator"
          image = "dr-orchestrator:latest"
          
          port {
            name           = "http"
            container_port = 8080
            protocol       = "TCP"
          }
          
          port {
            name           = "metrics"
            container_port = 9090
            protocol       = "TCP"
          }
          
          env {
            name  = "ENVIRONMENT"
            value = var.environment
          }
          
          env {
            name  = "RTO_TARGET_MINUTES"
            value = tostring(var.rto_minutes)
          }
          
          env {
            name  = "RPO_TARGET_MINUTES"
            value = tostring(var.rpo_minutes)
          }
          
          env {
            name  = "PRIMARY_CLOUD"
            value = var.primary_cloud
          }
          
          env {
            name  = "SECONDARY_CLOUD"
            value = var.secondary_cloud
          }
          
          env {
            name  = "ENABLE_AUTOMATED_FAILOVER"
            value = tostring(var.enable_automated_failover)
          }
          
          env {
            name = "DATABASE_PRIMARY_URL"
            value_from {
              secret_key_ref {
                name = var.database_secret_name
                key  = "primary_url"
              }
            }
          }
          
          env {
            name = "DATABASE_SECONDARY_URL"
            value_from {
              secret_key_ref {
                name = var.database_secret_name
                key  = "secondary_url"
              }
            }
          }
          
          env {
            name = "CLOUD_CREDENTIALS"
            value_from {
              secret_key_ref {
                name = var.cloud_credentials_secret_name
                key  = "credentials"
              }
            }
          }
          
          resources {
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
            limits = {
              cpu    = "2000m"
              memory = "4Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 3
          }
          
          readiness_probe {
            http_get {
              path = "/ready"
              port = 8080
            }
            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 3
            failure_threshold     = 3
          }
          
          volume_mount {
            name       = "config"
            mount_path = "/app/config"
            read_only  = true
          }
          
          volume_mount {
            name       = "dr-data"
            mount_path = "/app/data"
          }
        }
        
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.data_replication_config.metadata[0].name
          }
        }
        
        volume {
          name = "dr-data"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.dr_data.metadata[0].name
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# PVC for DR orchestrator data
resource "kubernetes_persistent_volume_claim" "dr_data" {
  metadata {
    name      = "${local.name_prefix}-dr-data"
    namespace = var.kubernetes_namespace
    labels    = var.tags
  }
  
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class
    
    resources {
      requests = {
        storage = "20Gi"
      }
    }
  }
}

# Service for DR orchestrator
resource "kubernetes_service" "dr_orchestrator" {
  metadata {
    name      = "${local.name_prefix}-dr-orchestrator-service"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "disaster-recovery"
    })
    annotations = {
      "prometheus.io/scrape" = "true"
      "prometheus.io/port"   = "9090"
      "prometheus.io/path"   = "/metrics"
    }
  }
  
  spec {
    selector = {
      app       = "${local.name_prefix}-dr-orchestrator"
      component = "disaster-recovery"
    }
    
    port {
      name        = "http"
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }
    
    port {
      name        = "metrics"
      port        = 9090
      target_port = 9090
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# ============================================================================
# CONTINUOUS DATA PROTECTION (CDP)
# ============================================================================

# CronJob for continuous data protection
resource "kubernetes_cron_job_v1" "continuous_data_protection" {
  metadata {
    name      = "${local.name_prefix}-cdp-job"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "continuous-data-protection"
      tier      = "backup"
    })
  }
  
  spec {
    schedule          = "*/5 * * * *"  # Every 5 minutes for RPO < 15 minutes
    concurrency_policy = "Forbid"
    
    job_template {
      metadata {
        labels = var.tags
      }
      
      spec {
        template {
          metadata {
            labels = var.tags
          }
          
          spec {
            service_account_name = var.service_account_name
            restart_policy      = "OnFailure"
            
            container {
              name  = "cdp-worker"
              image = "cdp-worker:latest"
              
              env {
                name  = "RPO_TARGET_MINUTES"
                value = tostring(var.rpo_minutes)
              }
              
              env {
                name  = "BACKUP_STRATEGY"
                value = var.rpo_minutes <= 15 ? "continuous" : "incremental"
              }
              
              env {
                name  = "CROSS_REGION_REPLICATION"
                value = tostring(var.enable_cross_cloud_replication)
              }
              
              env {
                name = "DATABASE_URL"
                value_from {
                  secret_key_ref {
                    name = var.database_secret_name
                    key  = "url"
                  }
                }
              }
              
              command = ["/opt/cdp/continuous-backup.sh"]
              
              resources {
                requests = {
                  cpu    = "250m"
                  memory = "512Mi"
                }
                limits = {
                  cpu    = "1000m"
                  memory = "2Gi"
                }
              }
              
              volume_mount {
                name       = "backup-storage"
                mount_path = "/backup"
              }
              
              volume_mount {
                name       = "wal-archive"
                mount_path = "/wal-archive"
              }
            }
            
            volume {
              name = "backup-storage"
              persistent_volume_claim {
                claim_name = kubernetes_persistent_volume_claim.backup_storage.metadata[0].name
              }
            }
            
            volume {
              name = "wal-archive"
              persistent_volume_claim {
                claim_name = kubernetes_persistent_volume_claim.wal_archive.metadata[0].name
              }
            }
          }
        }
        
        backoff_limit = 3
      }
    }
  }
}

# PVC for backup storage
resource "kubernetes_persistent_volume_claim" "backup_storage" {
  metadata {
    name      = "${local.name_prefix}-backup-storage"
    namespace = var.kubernetes_namespace
    labels    = var.tags
  }
  
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class
    
    resources {
      requests = {
        storage = var.environment == "prod" ? "1Ti" : "500Gi"
      }
    }
  }
}

# PVC for WAL archive
resource "kubernetes_persistent_volume_claim" "wal_archive" {
  metadata {
    name      = "${local.name_prefix}-wal-archive"
    namespace = var.kubernetes_namespace
    labels    = var.tags
  }
  
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = var.storage_class
    
    resources {
      requests = {
        storage = var.environment == "prod" ? "500Gi" : "200Gi"
      }
    }
  }
}

# ============================================================================
# DR TESTING AND VALIDATION
# ============================================================================

# CronJob for automated DR testing
resource "kubernetes_cron_job_v1" "dr_testing" {
  metadata {
    name      = "${local.name_prefix}-dr-testing"
    namespace = var.kubernetes_namespace
    labels = merge(var.tags, {
      component = "dr-testing"
      tier      = "validation"
    })
  }
  
  spec {
    schedule          = "0 6 * * 0"  # Weekly on Sunday at 6 AM
    concurrency_policy = "Forbid"
    
    job_template {
      metadata {
        labels = var.tags
      }
      
      spec {
        template {
          metadata {
            labels = var.tags
          }
          
          spec {
            service_account_name = var.service_account_name
            restart_policy      = "OnFailure"
            
            container {
              name  = "dr-tester"
              image = "dr-tester:latest"
              
              env {
                name  = "ENVIRONMENT"
                value = var.environment == "prod" ? "prod-dr-test" : var.environment
              }
              
              env {
                name  = "RTO_TARGET_MINUTES"
                value = tostring(var.rto_minutes)
              }
              
              env {
                name  = "RPO_TARGET_MINUTES"
                value = tostring(var.rpo_minutes)
              }
              
              env {
                name  = "TEST_SCENARIOS"
                value = "database_failover,application_failover,full_site_failover"
              }
              
              command = ["/opt/dr-test/run-dr-tests.sh"]
              
              resources {
                requests = {
                  cpu    = "500m"
                  memory = "1Gi"
                }
                limits = {
                  cpu    = "2000m"
                  memory = "4Gi"
                }
              }
              
              volume_mount {
                name       = "test-results"
                mount_path = "/test-results"
              }
            }
            
            volume {
              name = "test-results"
              empty_dir {}
            }
          }
        }
        
        backoff_limit = 2
      }
    }
  }
}

# ============================================================================
# DR MONITORING AND METRICS
# ============================================================================

# ServiceMonitor for DR metrics
resource "kubernetes_manifest" "dr_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "${local.name_prefix}-dr-monitoring"
      namespace = var.kubernetes_namespace
      labels = merge(var.tags, {
        component = "dr-monitoring"
      })
    }
    spec = {
      selector = {
        matchLabels = {
          component = "disaster-recovery"
        }
      }
      endpoints = [{
        port     = "metrics"
        interval = "30s"
        path     = "/metrics"
      }]
    }
  }
}

# PrometheusRule for DR alerting
resource "kubernetes_manifest" "dr_alerts" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "PrometheusRule"
    metadata = {
      name      = "${local.name_prefix}-dr-alerts"
      namespace = var.kubernetes_namespace
      labels = merge(var.tags, {
        component = "dr-alerting"
      })
    }
    spec = {
      groups = [
        {
          name = "disaster-recovery"
          rules = [
            {
              alert = "BackupJobFailure"
              expr  = "increase(backup_jobs_failed_total[5m]) > 0"
              for   = "1m"
              labels = {
                severity = "critical"
                team     = "infrastructure"
              }
              annotations = {
                summary     = "Backup job failed"
                description = "Backup job has failed, RPO may be at risk"
              }
            },
            {
              alert = "ReplicationLagHigh"
              expr  = "database_replication_lag_seconds > ${var.rpo_minutes * 60 * 0.8}"
              for   = "2m"
              labels = {
                severity = "warning"
                team     = "infrastructure"
              }
              annotations = {
                summary     = "Database replication lag is high"
                description = "Database replication lag exceeds 80% of RPO target"
              }
            },
            {
              alert = "DRTestFailure"
              expr  = "dr_test_success_rate < 0.95"
              for   = "5m"
              labels = {
                severity = "warning"
                team     = "infrastructure"
              }
              annotations = {
                summary     = "DR test failure rate is high"
                description = "DR test success rate is below 95%"
              }
            },
            {
              alert = "PrimaryRegionDown"
              expr  = "up{job=\"primary-region-health\"} == 0"
              for   = "3m"
              labels = {
                severity = "critical"
                team     = "infrastructure"
              }
              annotations = {
                summary     = "Primary region is down"
                description = "Primary region health check is failing, initiating failover"
              }
            }
          ]
        }
      ]
    }
  }
}

# ============================================================================
# SHARED RESOURCES
# ============================================================================

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# Data sources
data "aws_caller_identity" "current" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
}

data "aws_region" "current" {
  count = var.primary_cloud == "aws" || var.secondary_cloud == "aws" ? 1 : 0
}