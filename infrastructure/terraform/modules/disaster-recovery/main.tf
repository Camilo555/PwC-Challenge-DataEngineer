# BMAD Platform - Disaster Recovery Module
# 99.99% availability SLA with <1 hour RTO and automated failover

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
      version = "~> 4.80"
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

locals {
  common_tags = var.tags
  
  # SLA targets
  sla_config = {
    rto_minutes         = var.rto_target_minutes    # 60 minutes
    rpo_minutes         = var.rpo_target_minutes    # 15 minutes  
    availability_target = var.availability_target   # 99.99%
    downtime_budget     = (1 - var.availability_target) * 525600 # minutes per year
  }
  
  # Multi-cloud strategy
  cloud_strategy = {
    primary_cloud   = var.primary_cloud
    secondary_cloud = var.secondary_cloud
    tertiary_cloud  = var.tertiary_cloud
  }
  
  # Backup configuration
  backup_config = {
    cross_region_enabled = var.enable_cross_region_backup
    cross_cloud_enabled  = var.enable_cross_cloud_backup
    retention_days      = var.backup_retention_days
    replication_schedule = var.replication_schedule
  }
  
  # Replication settings
  replication_config = {
    database_replication = var.enable_database_replication
    storage_replication  = var.enable_storage_replication
    k8s_backup          = var.enable_k8s_backup
  }
}

# Disaster Recovery Namespace
resource "kubernetes_namespace" "disaster_recovery" {
  metadata {
    name = "disaster-recovery"
    
    labels = merge(local.common_tags, {
      purpose = "disaster-recovery"
      rto     = "${local.sla_config.rto_minutes}min"
      rpo     = "${local.sla_config.rpo_minutes}min"
      sla     = "${local.sla_config.availability_target * 100}%"
    })
  }
}

# AWS Disaster Recovery
# ====================

# AWS Cross-Region Database Replication
resource "aws_rds_cluster" "primary" {
  count = var.primary_cloud == "aws" ? 1 : 0
  
  cluster_identifier        = "${var.project_name}-primary-db"
  engine                   = "aurora-postgresql"
  engine_version           = "15.4"
  database_name            = var.project_name
  master_username          = "bmad_admin"
  manage_master_user_password = true
  
  backup_retention_period = local.backup_config.retention_days
  preferred_backup_window = "03:00-04:00"
  backup_window           = "03:00-04:00"
  
  # Multi-AZ deployment for high availability
  availability_zones = ["us-west-2a", "us-west-2b", "us-west-2c"]
  
  # Encryption at rest
  storage_encrypted = true
  kms_key_id       = var.aws_kms_key_id
  
  # Cross-region replication
  replication_source_identifier = null
  
  # Automated backups
  copy_tags_to_snapshot = true
  deletion_protection   = true
  
  # Performance monitoring
  enabled_cloudwatch_logs_exports = ["postgresql"]
  monitoring_interval             = 60
  monitoring_role_arn            = aws_iam_role.rds_monitoring[0].arn
  
  tags = merge(local.common_tags, {
    Purpose = "Primary Database"
    DR_Role = "Primary"
  })
}

# AWS Cross-Region Read Replica
resource "aws_rds_cluster" "dr_replica" {
  count = var.primary_cloud == "aws" && local.backup_config.cross_region_enabled ? 1 : 0
  
  cluster_identifier              = "${var.project_name}-dr-replica"
  replication_source_identifier   = aws_rds_cluster.primary[0].cluster_identifier
  engine                         = "aurora-postgresql"
  
  # Different region for disaster recovery
  availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]
  
  # Same encryption settings
  storage_encrypted = true
  kms_key_id       = var.aws_kms_key_id
  
  # Backup settings for DR
  backup_retention_period = local.backup_config.retention_days
  copy_tags_to_snapshot  = true
  deletion_protection    = true
  
  tags = merge(local.common_tags, {
    Purpose = "Disaster Recovery Database"
    DR_Role = "Secondary"
  })
  
  provider = aws.secondary_region
}

# IAM Role for RDS Monitoring
resource "aws_iam_role" "rds_monitoring" {
  count = var.primary_cloud == "aws" ? 1 : 0
  name  = "${var.project_name}-rds-monitoring"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "monitoring.rds.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

resource "aws_iam_role_policy_attachment" "rds_monitoring" {
  count      = var.primary_cloud == "aws" ? 1 : 0
  role       = aws_iam_role.rds_monitoring[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# S3 Cross-Region Replication for Data Lake
resource "aws_s3_bucket_replication_configuration" "data_lake_replication" {
  count = var.primary_cloud == "aws" && local.backup_config.cross_region_enabled ? 1 : 0
  
  role   = aws_iam_role.s3_replication[0].arn
  bucket = var.aws_s3_bucket_id
  
  rule {
    id     = "data-lake-dr-replication"
    status = "Enabled"
    
    destination {
      bucket        = aws_s3_bucket.dr_bucket[0].arn
      storage_class = "STANDARD_IA"
      
      encryption_configuration {
        replica_kms_key_id = var.aws_kms_key_id
      }
    }
  }
  
  depends_on = [aws_s3_bucket_versioning.data_lake_versioning]
}

# DR S3 Bucket
resource "aws_s3_bucket" "dr_bucket" {
  count  = var.primary_cloud == "aws" && local.backup_config.cross_region_enabled ? 1 : 0
  bucket = "${var.project_name}-dr-data-lake"
  
  tags = merge(local.common_tags, {
    Purpose = "Disaster Recovery Data Lake"
    DR_Role = "Secondary"
  })
  
  provider = aws.secondary_region
}

# S3 Replication IAM Role
resource "aws_iam_role" "s3_replication" {
  count = var.primary_cloud == "aws" && local.backup_config.cross_region_enabled ? 1 : 0
  name  = "${var.project_name}-s3-replication"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
      }
    ]
  })
  
  tags = local.common_tags
}

# Azure Disaster Recovery
# ======================

# Azure Site Recovery Vault
resource "azurerm_recovery_services_vault" "dr_vault" {
  count = var.secondary_cloud == "azure" ? 1 : 0
  
  name                = "${var.project_name}-dr-vault"
  location            = var.azure_secondary_region
  resource_group_name = var.azure_resource_group_name
  sku                 = "Standard"
  
  storage_mode_type         = "GeoRedundant"
  cross_region_restore_enabled = true
  
  tags = local.common_tags
}

# Azure SQL Database Geo-Replication
resource "azurerm_sql_failover_group" "sql_failover" {
  count = var.secondary_cloud == "azure" && local.replication_config.database_replication ? 1 : 0
  
  name                = "${var.project_name}-sql-failover"
  resource_group_name = var.azure_resource_group_name
  server_name         = var.azure_sql_server_name
  
  partner_servers {
    id = azurerm_mssql_server.dr_server[0].id
  }
  
  read_write_endpoint_failover_policy {
    mode          = "Automatic"
    grace_minutes = local.sla_config.rto_minutes
  }
  
  readonly_endpoint_failover_policy {
    mode = "Enabled"
  }
  
  tags = local.common_tags
}

# Azure DR SQL Server
resource "azurerm_mssql_server" "dr_server" {
  count = var.secondary_cloud == "azure" && local.replication_config.database_replication ? 1 : 0
  
  name                         = "${var.project_name}-dr-sql-server"
  resource_group_name          = var.azure_resource_group_name
  location                     = var.azure_secondary_region
  version                      = "12.0"
  administrator_login          = "bmad_admin"
  administrator_login_password = var.azure_sql_admin_password
  
  tags = local.common_tags
}

# Google Cloud Disaster Recovery
# ==============================

# Cloud SQL Cross-Region Replica
resource "google_sql_database_instance" "dr_replica" {
  count = var.tertiary_cloud == "gcp" && local.replication_config.database_replication ? 1 : 0
  
  name             = "${var.project_name}-dr-replica"
  database_version = "POSTGRES_15"
  region          = var.gcp_secondary_region
  
  master_instance_name = var.gcp_primary_instance_name
  
  replica_configuration {
    failover_target = true
  }
  
  settings {
    tier = "db-n1-standard-2"
    
    backup_configuration {
      enabled                        = true
      start_time                     = "03:00"
      point_in_time_recovery_enabled = true
      backup_retention_settings {
        retained_backups = local.backup_config.retention_days
      }
    }
    
    ip_configuration {
      ipv4_enabled = false
      private_network = var.gcp_vpc_network
    }
    
    database_flags {
      name  = "log_statement"
      value = "all"
    }
  }
  
  deletion_protection = true
}

# Kubernetes Disaster Recovery
# ============================

# Velero Backup Solution
resource "helm_release" "velero" {
  name       = "velero"
  repository = "https://vmware-tanzu.github.io/helm-charts"
  chart      = "velero"
  version    = "5.1.4"
  namespace  = kubernetes_namespace.disaster_recovery.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/velero-values.yaml", {
      backup_location     = "s3"
      s3_bucket          = "${var.project_name}-k8s-backups"
      s3_region          = "us-west-2"
      backup_retention   = "${local.backup_config.retention_days * 24}h"
      schedule_cron      = local.backup_config.replication_schedule
      snapshot_volumes   = true
      default_volumes_to_restic = true
      restic_timeout     = "6h"
    })
  ]
  
  depends_on = [kubernetes_namespace.disaster_recovery]
}

# Cluster Backup CronJob
resource "kubernetes_cron_job" "cluster_backup" {
  count = local.replication_config.k8s_backup ? 1 : 0
  
  metadata {
    name      = "cluster-backup"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  spec {
    concurrency_policy = "Forbid"
    schedule          = local.backup_config.replication_schedule
    
    job_template {
      metadata {
        name = "cluster-backup"
      }
      
      spec {
        template {
          metadata {
            name = "cluster-backup"
          }
          
          spec {
            restart_policy = "OnFailure"
            
            container {
              name  = "backup"
              image = "velero/velero:v1.12.1"
              
              command = [
                "velero",
                "backup",
                "create",
                "scheduled-backup-$(date +%Y%m%d%H%M%S)",
                "--include-namespaces=default,vector-db,security-system",
                "--ttl=${local.backup_config.retention_days * 24}h"
              ]
              
              env {
                name  = "AWS_SHARED_CREDENTIALS_FILE"
                value = "/credentials/cloud"
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
              
              volume_mount {
                name       = "cloud-credentials"
                mount_path = "/credentials"
                read_only  = true
              }
            }
            
            volume {
              name = "cloud-credentials"
              secret {
                secret_name = kubernetes_secret.backup_credentials.metadata[0].name
              }
            }
          }
        }
      }
    }
  }
}

# Backup Credentials Secret
resource "kubernetes_secret" "backup_credentials" {
  metadata {
    name      = "backup-credentials"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
  
  data = {
    cloud = base64encode(templatefile("${path.module}/templates/cloud-credentials.tpl", {
      aws_access_key_id     = var.aws_backup_access_key
      aws_secret_access_key = var.aws_backup_secret_key
      azure_subscription_id = var.azure_subscription_id
      azure_tenant_id      = var.azure_tenant_id
      azure_client_id      = var.azure_client_id
      azure_client_secret  = var.azure_client_secret
      gcp_service_account   = var.gcp_service_account_key
    }))
  }
  
  type = "Opaque"
}

# Disaster Recovery Automation
resource "kubernetes_deployment" "dr_controller" {
  metadata {
    name      = "dr-controller"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
    
    labels = {
      app = "dr-controller"
    }
  }

  spec {
    replicas = 2

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
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        container {
          image = "bmad/dr-controller:v1.0.0"
          name  = "dr-controller"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name  = "RTO_TARGET_MINUTES"
            value = tostring(local.sla_config.rto_minutes)
          }
          
          env {
            name  = "RPO_TARGET_MINUTES"
            value = tostring(local.sla_config.rpo_minutes)
          }
          
          env {
            name  = "AVAILABILITY_TARGET"
            value = tostring(local.sla_config.availability_target)
          }
          
          env {
            name  = "PRIMARY_CLOUD"
            value = local.cloud_strategy.primary_cloud
          }
          
          env {
            name  = "SECONDARY_CLOUD"
            value = local.cloud_strategy.secondary_cloud
          }
          
          env {
            name  = "TERTIARY_CLOUD"
            value = local.cloud_strategy.tertiary_cloud
          }
          
          env {
            name  = "BACKUP_RETENTION_DAYS"
            value = tostring(local.backup_config.retention_days)
          }
          
          env {
            name  = "HEALTH_CHECK_INTERVAL"
            value = "60"  # seconds
          }
          
          env {
            name  = "FAILOVER_WEBHOOK_URL"
            value = var.failover_webhook_url
          }
          
          resources {
            requests = {
              cpu    = "200m"
              memory = "512Mi"
            }
            limits = {
              cpu    = "1000m"
              memory = "2Gi"
            }
          }
          
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
          
          volume_mount {
            name       = "dr-config"
            mount_path = "/etc/dr"
            read_only  = true
          }
        }
        
        volume {
          name = "dr-config"
          config_map {
            name = kubernetes_config_map.dr_config.metadata[0].name
          }
        }
        
        service_account_name = kubernetes_service_account.dr_controller.metadata[0].name
      }
    }
  }
}

# DR Configuration
resource "kubernetes_config_map" "dr_config" {
  metadata {
    name      = "dr-config"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }

  data = {
    "failover-plan.yaml" = templatefile("${path.module}/templates/failover-plan.yaml.tpl", {
      rto_minutes         = local.sla_config.rto_minutes
      rpo_minutes         = local.sla_config.rpo_minutes
      primary_cloud       = local.cloud_strategy.primary_cloud
      secondary_cloud     = local.cloud_strategy.secondary_cloud
      tertiary_cloud      = local.cloud_strategy.tertiary_cloud
      automatic_failover  = true
      manual_approval     = false
    })
    
    "recovery-procedures.yaml" = templatefile("${path.module}/templates/recovery-procedures.yaml.tpl", {
      database_recovery_steps = [
        "Verify secondary database health",
        "Promote read replica to primary",
        "Update application connection strings",
        "Verify data consistency"
      ]
      application_recovery_steps = [
        "Scale up secondary region clusters",
        "Update DNS records",
        "Verify application health",
        "Monitor performance metrics"
      ]
      network_recovery_steps = [
        "Activate secondary load balancers",
        "Update CDN origins", 
        "Verify SSL certificates",
        "Test end-to-end connectivity"
      ]
    })
    
    "monitoring-thresholds.yaml" = templatefile("${path.module}/templates/monitoring-thresholds.yaml.tpl", {
      availability_threshold     = local.sla_config.availability_target
      response_time_threshold_ms = 5000
      error_rate_threshold      = 0.01  # 1%
      database_lag_threshold_sec = local.sla_config.rpo_minutes * 60
    })
  }
}

# Service Account for DR Controller
resource "kubernetes_service_account" "dr_controller" {
  metadata {
    name      = "dr-controller"
    namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
  }
}

# ClusterRole for DR Controller
resource "kubernetes_cluster_role" "dr_controller" {
  metadata {
    name = "dr-controller"
  }

  rule {
    api_groups = [""]
    resources  = ["*"]
    verbs      = ["*"]
  }
  
  rule {
    api_groups = ["apps", "extensions", "batch"]
    resources  = ["*"]
    verbs      = ["*"]
  }
  
  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["*"]
    verbs      = ["*"]
  }
}

# ClusterRoleBinding for DR Controller
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
    
    labels = {
      app = "dr-controller"
    }
  }
  
  spec {
    selector = {
      app = "dr-controller"
    }
    
    port {
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# DR Monitoring ServiceMonitor
resource "kubernetes_manifest" "dr_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "dr-monitoring"
      namespace = kubernetes_namespace.disaster_recovery.metadata[0].name
      labels = {
        app = "dr-controller"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "dr-controller"
        }
      }
      endpoints = [
        {
          port     = "http"
          path     = "/metrics"
          interval = "30s"
        }
      ]
    }
  }
}