# Disaster Recovery Module
# Comprehensive multi-cloud disaster recovery with automated failover
# Features: Cross-cloud replication, automated recovery, <15min RTO, <5min RPO

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
  }
  required_version = ">= 1.5"
}

# Local variables for DR configuration
locals {
  disaster_recovery_regions = {
    aws = {
      primary   = var.aws_primary_region
      secondary = var.aws_dr_region
    }
    azure = {
      primary   = var.azure_primary_region
      secondary = var.azure_dr_region
    }
    gcp = {
      primary   = var.gcp_primary_region
      secondary = var.gcp_dr_region
    }
  }
  
  backup_retention = {
    critical = var.backup_retention_critical_days
    standard = var.backup_retention_standard_days
    archive  = var.backup_retention_archive_days
  }
}

# AWS Disaster Recovery Configuration
module "aws_disaster_recovery" {
  source = "./aws"
  count  = var.enable_aws_dr ? 1 : 0
  
  project_name = var.project_name
  environment  = var.environment
  
  primary_region   = local.disaster_recovery_regions.aws.primary
  secondary_region = local.disaster_recovery_regions.aws.secondary
  
  # RTO/RPO requirements
  rto_minutes = var.rto_target_minutes
  rpo_minutes = var.rpo_target_minutes
  
  # Backup configuration
  backup_retention_days = local.backup_retention
  enable_point_in_time_recovery = var.enable_pitr
  
  # Cross-region replication
  enable_cross_region_replication = var.enable_cross_region_replication
  replication_schedule = var.replication_schedule
  
  # Automated failover
  enable_automated_failover = var.enable_automated_failover
  health_check_grace_period = var.health_check_grace_period_seconds
  
  # Data sources
  primary_vpc_id = var.aws_primary_vpc_id
  secondary_vpc_id = var.aws_secondary_vpc_id
  
  # Database instances
  database_instances = var.aws_database_instances
  
  # S3 buckets for data lake
  data_lake_buckets = var.aws_data_lake_buckets
  
  common_tags = var.common_tags
}

# Azure Disaster Recovery Configuration
module "azure_disaster_recovery" {
  source = "./azure"
  count  = var.enable_azure_dr ? 1 : 0
  
  project_name = var.project_name
  environment  = var.environment
  
  primary_region   = local.disaster_recovery_regions.azure.primary
  secondary_region = local.disaster_recovery_regions.azure.secondary
  
  # Resource groups
  primary_resource_group_name   = var.azure_primary_resource_group
  secondary_resource_group_name = var.azure_secondary_resource_group
  
  # RTO/RPO requirements
  rto_minutes = var.rto_target_minutes
  rpo_minutes = var.rpo_target_minutes
  
  # Backup configuration
  backup_retention_days = local.backup_retention
  enable_geo_redundant_backup = var.enable_geo_redundant_backup
  
  # Azure Site Recovery
  enable_site_recovery = var.enable_site_recovery
  recovery_vault_name = var.azure_recovery_vault_name
  
  # Storage accounts
  storage_accounts = var.azure_storage_accounts
  
  # SQL databases
  sql_databases = var.azure_sql_databases
  
  common_tags = var.common_tags
}

# GCP Disaster Recovery Configuration
module "gcp_disaster_recovery" {
  source = "./gcp"
  count  = var.enable_gcp_dr ? 1 : 0
  
  project_name = var.project_name
  environment  = var.environment
  project_id   = var.gcp_project_id
  
  primary_region   = local.disaster_recovery_regions.gcp.primary
  secondary_region = local.disaster_recovery_regions.gcp.secondary
  
  # RTO/RPO requirements
  rto_minutes = var.rto_target_minutes
  rpo_minutes = var.rpo_target_minutes
  
  # Backup configuration
  backup_retention_days = local.backup_retention
  enable_continuous_backup = var.enable_continuous_backup
  
  # Cloud SQL instances
  cloud_sql_instances = var.gcp_cloud_sql_instances
  
  # Cloud Storage buckets
  storage_buckets = var.gcp_storage_buckets
  
  # Compute instances
  compute_instances = var.gcp_compute_instances
  
  common_tags = var.common_tags
}

# Cross-Cloud Disaster Recovery Orchestration
resource "kubernetes_namespace" "disaster_recovery" {
  metadata {
    name = "disaster-recovery"
    labels = {
      name = "disaster-recovery"
      purpose = "dr-orchestration"
    }
  }
}

# Disaster Recovery Controller
resource "kubernetes_deployment" "dr_controller" {
  metadata {
    name      = "dr-controller"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
    labels = {
      app = "dr-controller"
    }
  }
  
  spec {
    replicas = var.dr_controller_replicas
    
    selector {
      match_labels = {
        app = "dr-controller"
      }
    }
    
    template {
      metadata {
        labels = {
          app = "dr-controller"
        }
      }
      
      spec {
        service_account_name = kubernetes_service_account.dr_controller.metadata[0].name
        
        container {
          name  = "dr-controller"
          image = var.dr_controller_image
          
          env {
            name = "RTO_TARGET_MINUTES"
            value = tostring(var.rto_target_minutes)
          }
          
          env {
            name = "RPO_TARGET_MINUTES"
            value = tostring(var.rpo_target_minutes)
          }
          
          env {
            name = "CHECK_INTERVAL_SECONDS"
            value = tostring(var.health_check_interval_seconds)
          }
          
          env {
            name = "FAILOVER_THRESHOLD"
            value = tostring(var.failover_threshold_failures)
          }
          
          # AWS credentials
          env {
            name = "AWS_ACCESS_KEY_ID"
            value_from {
              secret_key_ref {
                name = "cloud-credentials"
                key  = "aws_access_key_id"
              }
            }
          }
          
          env {
            name = "AWS_SECRET_ACCESS_KEY"
            value_from {
              secret_key_ref {
                name = "cloud-credentials"
                key  = "aws_secret_access_key"
              }
            }
          }
          
          # Azure credentials
          env {
            name = "AZURE_CLIENT_ID"
            value_from {
              secret_key_ref {
                name = "cloud-credentials"
                key  = "azure_client_id"
              }
            }
          }
          
          env {
            name = "AZURE_CLIENT_SECRET"
            value_from {
              secret_key_ref {
                name = "cloud-credentials"
                key  = "azure_client_secret"
              }
            }
          }
          
          # GCP credentials
          env {
            name = "GOOGLE_APPLICATION_CREDENTIALS"
            value = "/var/secrets/google/key.json"
          }
          
          volume_mount {
            name       = "google-cloud-key"
            mount_path = "/var/secrets/google"
            read_only  = true
          }
          
          # Health checks
          liveness_probe {
            http_get {
              path = "/health"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/ready"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
          
          resources {
            requests = {
              cpu    = "100m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }
        }
        
        volume {
          name = "google-cloud-key"
          secret {
            secret_name = "google-cloud-key"
          }
        }
      }
    }
  }
}

# Service Account for DR Controller
resource "kubernetes_service_account" "dr_controller" {
  metadata {
    name      = "dr-controller"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
}

# RBAC for DR Controller
resource "kubernetes_cluster_role" "dr_controller" {
  metadata {
    name = "dr-controller"
  }
  
  rule {
    api_groups = [""]
    resources  = ["nodes", "pods", "services", "endpoints", "persistentvolumeclaims", "persistentvolumes"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "statefulsets", "replicasets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  
  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["ingresses"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "dr_controller" {
  metadata {
    name = "dr-controller"
  }
  
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.dr_controller.metadata[0].name
  }
  
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.dr_controller.metadata[0].name
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
}

# Health Check Service
resource "kubernetes_service" "dr_controller" {
  metadata {
    name      = "dr-controller"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  spec {
    selector = {
      app = "dr-controller"
    }
    
    port {
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# Disaster Recovery Configuration Map
resource "kubernetes_config_map" "dr_config" {
  metadata {
    name      = "dr-config"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  data = {
    "config.yaml" = yamlencode({
      disaster_recovery = {
        rto_target_minutes = var.rto_target_minutes
        rpo_target_minutes = var.rpo_target_minutes
        
        health_checks = {
          interval_seconds = var.health_check_interval_seconds
          timeout_seconds  = var.health_check_timeout_seconds
          failure_threshold = var.failover_threshold_failures
          grace_period_seconds = var.health_check_grace_period_seconds
        }
        
        failover = {
          enabled = var.enable_automated_failover
          strategy = var.failover_strategy
          notification_webhooks = var.failover_notification_webhooks
        }
        
        backup = {
          retention_days = local.backup_retention
          schedule = var.backup_schedule
          encryption_enabled = var.enable_backup_encryption
        }
        
        replication = {
          enabled = var.enable_cross_region_replication
          schedule = var.replication_schedule
          compression_enabled = var.enable_replication_compression
        }
        
        clouds = {
          aws = {
            enabled = var.enable_aws_dr
            primary_region = local.disaster_recovery_regions.aws.primary
            secondary_region = local.disaster_recovery_regions.aws.secondary
          }
          
          azure = {
            enabled = var.enable_azure_dr
            primary_region = local.disaster_recovery_regions.azure.primary
            secondary_region = local.disaster_recovery_regions.azure.secondary
          }
          
          gcp = {
            enabled = var.enable_gcp_dr
            primary_region = local.disaster_recovery_regions.gcp.primary
            secondary_region = local.disaster_recovery_regions.gcp.secondary
          }
        }
      }
    })
  }
}

# Backup and Recovery CronJobs
resource "kubernetes_cron_job_v1" "backup_validation" {
  metadata {
    name      = "backup-validation"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  spec {
    schedule = var.backup_validation_schedule
    
    job_template {
      metadata {
        labels = {
          app = "backup-validation"
        }
      }
      
      spec {
        template {
          metadata {
            labels = {
              app = "backup-validation"
            }
          }
          
          spec {
            container {
              name  = "backup-validator"
              image = var.backup_validator_image
              
              command = ["/bin/sh", "-c"]
              args = [
                <<EOF
                  echo "Starting backup validation..."
                  
                  # Validate AWS backups
                  if [ "$AWS_DR_ENABLED" = "true" ]; then
                    echo "Validating AWS backups..."
                    python /scripts/validate_aws_backups.py
                  fi
                  
                  # Validate Azure backups
                  if [ "$AZURE_DR_ENABLED" = "true" ]; then
                    echo "Validating Azure backups..."
                    python /scripts/validate_azure_backups.py
                  fi
                  
                  # Validate GCP backups
                  if [ "$GCP_DR_ENABLED" = "true" ]; then
                    echo "Validating GCP backups..."
                    python /scripts/validate_gcp_backups.py
                  fi
                  
                  echo "Backup validation completed"
                EOF
              ]
              
              env {
                name = "AWS_DR_ENABLED"
                value = tostring(var.enable_aws_dr)
              }
              
              env {
                name = "AZURE_DR_ENABLED"
                value = tostring(var.enable_azure_dr)
              }
              
              env {
                name = "GCP_DR_ENABLED"
                value = tostring(var.enable_gcp_dr)
              }
            }
            
            restart_policy = "OnFailure"
          }
        }
        
        backoff_limit = 3
      }
    }
    
    successful_jobs_history_limit = 3
    failed_jobs_history_limit     = 3
  }
}

# Disaster Recovery Testing CronJob
resource "kubernetes_cron_job_v1" "dr_testing" {
  metadata {
    name      = "dr-testing"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  spec {
    schedule = var.dr_testing_schedule
    
    job_template {
      metadata {
        labels = {
          app = "dr-testing"
        }
      }
      
      spec {
        template {
          metadata {
            labels = {
              app = "dr-testing"
            }
          }
          
          spec {
            container {
              name  = "dr-tester"
              image = var.dr_testing_image
              
              command = ["/bin/sh", "-c"]
              args = [
                <<EOF
                  echo "Starting DR testing..."
                  
                  # Test failover mechanisms
                  echo "Testing failover mechanisms..."
                  python /scripts/test_failover.py
                  
                  # Test backup restore
                  echo "Testing backup restore..."
                  python /scripts/test_restore.py --dry-run
                  
                  # Test cross-region connectivity
                  echo "Testing cross-region connectivity..."
                  python /scripts/test_connectivity.py
                  
                  # Generate test report
                  python /scripts/generate_dr_report.py
                  
                  echo "DR testing completed"
                EOF
              ]
            }
            
            restart_policy = "OnFailure"
          }
        }
        
        backoff_limit = 1
      }
    }
    
    successful_jobs_history_limit = 5
    failed_jobs_history_limit     = 5
  }
}

# Monitoring and Alerting for DR
resource "kubernetes_service_monitor" "dr_metrics" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "dr-metrics"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
    labels = {
      app = "dr-metrics"
    }
  }
  
  spec {
    selector {
      match_labels = {
        app = "dr-controller"
      }
    }
    
    endpoints {
      port = "metrics"
      path = "/metrics"
      interval = "30s"
    }
  }
}

# Prometheus Rules for DR Alerting
resource "kubernetes_config_map" "dr_prometheus_rules" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "dr-prometheus-rules"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
    labels = {
      prometheus = "kube-prometheus"
      role       = "alert-rules"
    }
  }
  
  data = {
    "disaster-recovery.yaml" = yamlencode({
      groups = [
        {
          name = "disaster-recovery"
          rules = [
            {
              alert = "DRControllerDown"
              expr  = "up{job=\"dr-controller\"} == 0"
              for   = "5m"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "DR Controller is down"
                description = "The Disaster Recovery Controller has been down for more than 5 minutes"
              }
            },
            {
              alert = "BackupFailure"
              expr  = "increase(backup_failures_total[1h]) > 0"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "Backup failure detected"
                description = "One or more backups have failed in the last hour"
              }
            },
            {
              alert = "ReplicationLag"
              expr  = "replication_lag_seconds > 300"
              labels = {
                severity = "warning"
              }
              annotations = {
                summary     = "High replication lag"
                description = "Replication lag is higher than 5 minutes (current: {{ $value }}s)"
              }
            },
            {
              alert = "RTOViolation"
              expr  = "recovery_time_seconds > ${var.rto_target_minutes * 60}"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "RTO target violated"
                description = "Recovery took longer than RTO target of ${var.rto_target_minutes} minutes"
              }
            },
            {
              alert = "RPOViolation"
              expr  = "data_loss_seconds > ${var.rpo_target_minutes * 60}"
              labels = {
                severity = "critical"
              }
              annotations = {
                summary     = "RPO target violated"
                description = "Data loss exceeds RPO target of ${var.rpo_target_minutes} minutes"
              }
            }
          ]
        }
      ]
    })
  }
}

# Grafana Dashboard for DR Monitoring
resource "kubernetes_config_map" "dr_grafana_dashboard" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "dr-grafana-dashboard"
    namespace = "monitoring"
    labels = {
      grafana_dashboard = "1"
    }
  }
  
  data = {
    "disaster-recovery.json" = jsonencode({
      dashboard = {
        title = "Disaster Recovery Dashboard"
        panels = [
          {
            title = "RTO/RPO Metrics"
            type  = "stat"
            targets = [
              {
                expr = "recovery_time_seconds"
                legendFormat = "Recovery Time (seconds)"
              },
              {
                expr = "data_loss_seconds"
                legendFormat = "Data Loss (seconds)"
              }
            ]
          },
          {
            title = "Backup Status"
            type  = "table"
            targets = [
              {
                expr = "backup_status"
                legendFormat = "{{ cloud }} - {{ region }}"
              }
            ]
          },
          {
            title = "Replication Lag"
            type  = "graph"
            targets = [
              {
                expr = "replication_lag_seconds"
                legendFormat = "{{ source }} -> {{ destination }}"
              }
            ]
          },
          {
            title = "Failover Events"
            type  = "graph"
            targets = [
              {
                expr = "increase(failover_events_total[24h])"
                legendFormat = "Failovers (24h)"
              }
            ]
          }
        ]
      }
    })
  }
}

# Emergency Runbook ConfigMap
resource "kubernetes_config_map" "dr_runbook" {
  metadata {
    name      = "dr-runbook"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  data = {
    "emergency-procedures.md" = <<EOF
# Disaster Recovery Emergency Procedures

## Quick Reference
- **RTO Target**: ${var.rto_target_minutes} minutes
- **RPO Target**: ${var.rpo_target_minutes} minutes
- **Emergency Contact**: ${var.emergency_contact}
- **Escalation Path**: ${var.escalation_path}

## Immediate Actions (First 5 minutes)
1. Assess the scope of the outage
2. Activate the disaster recovery team
3. Initiate communications to stakeholders
4. Begin failover procedures

## Failover Procedures
### Automated Failover
- Monitor DR Controller logs: `kubectl logs -f deployment/dr-controller -n disaster-recovery`
- Check failover status: `kubectl get configmap dr-status -n disaster-recovery`

### Manual Failover
1. **AWS Failover**:
   - RDS: `aws rds failover-db-cluster --db-cluster-identifier <cluster-id>`
   - Route53: Update DNS records to secondary region
   - Application: Scale up secondary region instances

2. **Azure Failover**:
   - SQL Database: Initiate geo-failover in Azure portal
   - Traffic Manager: Update endpoints
   - App Services: Scale up secondary region

3. **GCP Failover**:
   - Cloud SQL: `gcloud sql instances failover <instance-id>`
   - Cloud Load Balancing: Update backend services
   - Compute Engine: Start secondary region instances

## Recovery Procedures
1. **Data Validation**:
   - Verify data integrity in secondary region
   - Check replication lag: < ${var.rpo_target_minutes} minutes
   - Validate backup completeness

2. **Application Health**:
   - Run health checks: `curl -f <endpoint>/health`
   - Monitor error rates and response times
   - Verify all critical services are operational

3. **User Communication**:
   - Update status page
   - Send notification to affected users
   - Provide estimated time to full recovery

## Testing and Validation
- Monthly DR tests: ${var.dr_testing_schedule}
- Backup validation: ${var.backup_validation_schedule}
- RTO/RPO compliance reporting

## Contact Information
- **Primary On-Call**: ${var.primary_oncall}
- **Secondary On-Call**: ${var.secondary_oncall}
- **Management Escalation**: ${var.management_escalation}
EOF
  }
}