# Data Platform Module - Cloud-Native Lakehouse Architecture
# Supports multi-cloud deployment with Delta Lake, Apache Iceberg, and Hudi

terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
}

# Local variables for data platform configuration
locals {
  # Data platform components
  platform_components = {
    "spark-operator"     = var.env_config.cluster_size != "small"
    "delta-lake"        = true
    "iceberg-catalog"   = var.env_config.cluster_size == "large"
    "hudi-streamer"     = var.enable_streaming
    "data-catalog"      = true
    "quality-monitor"   = true
    "lineage-tracker"   = var.env_config.monitoring_level != "basic"
  }
  
  # Storage tier configurations
  storage_tiers = {
    "hot" = {
      retention_days = 30
      access_tier    = "hot"
      replication    = 3
    }
    "warm" = {
      retention_days = 365
      access_tier    = "cool"
      replication    = 2
    }
    "cold" = {
      retention_days = 2555  # 7 years
      access_tier    = "archive"
      replication    = 1
    }
  }
}

# Spark Operator for distributed processing
resource "helm_release" "spark_operator" {
  count = local.platform_components["spark-operator"] ? 1 : 0

  name       = "spark-operator"
  repository = "https://googlecloudplatform.github.io/spark-on-k8s-operator"
  chart      = "spark-operator"
  version    = "1.1.27"
  namespace  = "spark-system"
  
  create_namespace = true

  values = [
    yamlencode({
      image = {
        repository = "gcr.io/spark-operator/spark-operator"
        tag        = "v1beta2-1.3.8-3.1.1"
      }
      
      webhook = {
        enable = true
        port   = 8080
      }
      
      metrics = {
        enable = true
        port   = 10254
      }
      
      # Resource management
      resources = var.env_config.cluster_size == "large" ? {
        limits = {
          cpu    = "2"
          memory = "2Gi"
        }
        requests = {
          cpu    = "1"
          memory = "1Gi"
        }
      } : {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        requests = {
          cpu    = "500m"
          memory = "512Mi"
        }
      }
      
      # Security configuration
      securityContext = {
        runAsNonRoot = true
        runAsUser    = 1000
        fsGroup      = 1000
      }
      
      # Node affinity for cost optimization
      nodeSelector = {
        "node.kubernetes.io/instance-type" = var.env_config.cluster_size == "large" ? "c5.2xlarge" : "t3.large"
      }
      
      tolerations = var.enable_spot_instances ? [
        {
          key      = "node.kubernetes.io/spot"
          operator = "Equal"
          value    = "true"
          effect   = "NoSchedule"
        }
      ] : []
    })
  ]

  depends_on = [kubernetes_namespace.data_platform]
}

# Data Catalog using Apache Atlas or custom solution
resource "kubernetes_deployment" "data_catalog" {
  count = local.platform_components["data-catalog"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-data-catalog"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = merge(var.common_tags, {
      component = "data-catalog"
      tier      = "control-plane"
    })
  }

  spec {
    replicas = var.env_config.high_availability ? 3 : 1

    selector {
      match_labels = {
        app = "${var.resource_prefix}-data-catalog"
      }
    }

    template {
      metadata {
        labels = merge({
          app = "${var.resource_prefix}-data-catalog"
        }, var.common_tags)
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.data_platform.metadata[0].name
        
        security_context {
          run_as_non_root = true
          run_as_user     = 1000
          fs_group        = 1000
        }

        container {
          name  = "data-catalog"
          image = "apache/atlas:2.3.0"  # Can be customized based on cloud provider
          
          port {
            name           = "http"
            container_port = 21000
            protocol       = "TCP"
          }
          
          port {
            name           = "admin"
            container_port = 8080
            protocol       = "TCP"
          }

          env {
            name  = "ATLAS_SERVER_OPTS"
            value = "-Xmx${var.env_config.cluster_size == "large" ? "4" : "2"}g -Xms${var.env_config.cluster_size == "large" ? "2" : "1"}g"
          }

          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.data_platform_secrets.metadata[0].name
                key  = "catalog-database-url"
              }
            }
          }

          resources {
            limits = {
              cpu    = var.env_config.cluster_size == "large" ? "2" : "1"
              memory = var.env_config.cluster_size == "large" ? "4Gi" : "2Gi"
            }
            requests = {
              cpu    = var.env_config.cluster_size == "large" ? "1" : "500m"
              memory = var.env_config.cluster_size == "large" ? "2Gi" : "1Gi"
            }
          }

          liveness_probe {
            http_get {
              path = "/api/atlas/admin/status"
              port = 21000
            }
            initial_delay_seconds = 60
            period_seconds       = 30
            timeout_seconds      = 10
            failure_threshold    = 3
          }

          readiness_probe {
            http_get {
              path = "/api/atlas/admin/status"
              port = 21000
            }
            initial_delay_seconds = 30
            period_seconds       = 10
            timeout_seconds      = 5
            failure_threshold    = 2
          }

          volume_mount {
            name       = "data-storage"
            mount_path = "/opt/atlas/data"
          }
          
          volume_mount {
            name       = "config"
            mount_path = "/opt/atlas/conf"
            read_only  = true
          }
        }

        volume {
          name = "data-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.catalog_storage[0].metadata[0].name
          }
        }
        
        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.data_catalog_config[0].metadata[0].name
          }
        }

        # Anti-affinity for high availability
        dynamic "affinity" {
          for_each = var.env_config.high_availability ? [1] : []
          content {
            pod_anti_affinity {
              preferred_during_scheduling_ignored_during_execution {
                weight = 100
                pod_affinity_term {
                  label_selector {
                    match_labels = {
                      app = "${var.resource_prefix}-data-catalog"
                    }
                  }
                  topology_key = "kubernetes.io/hostname"
                }
              }
            }
          }
        }
      }
    }
  }
}

# Data Quality Monitor using Great Expectations
resource "kubernetes_deployment" "data_quality_monitor" {
  count = local.platform_components["quality-monitor"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-data-quality"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = merge(var.common_tags, {
      component = "data-quality"
      tier      = "monitoring"
    })
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "${var.resource_prefix}-data-quality"
      }
    }

    template {
      metadata {
        labels = {
          app = "${var.resource_prefix}-data-quality"
        }
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.data_platform.metadata[0].name
        
        security_context {
          run_as_non_root = true
          run_as_user     = 1000
          fs_group        = 1000
        }

        container {
          name  = "data-quality-monitor"
          image = "greatexpectations/great_expectations:latest"
          
          port {
            name           = "http"
            container_port = 8080
            protocol       = "TCP"
          }

          env {
            name  = "GE_USAGE_STATS_ENABLED"
            value = "false"
          }

          env {
            name = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.data_platform_secrets.metadata[0].name
                key  = "database-url"
              }
            }
          }

          # Cloud-specific data source configurations
          dynamic "env" {
            for_each = var.cloud_provider == "aws" && var.aws_data_sources != null ? [1] : []
            content {
              name  = "AWS_S3_BUCKET"
              value = var.aws_data_sources.s3_bucket
            }
          }

          dynamic "env" {
            for_each = var.cloud_provider == "azure" && var.azure_data_sources != null ? [1] : []
            content {
              name  = "AZURE_STORAGE_ACCOUNT"
              value = var.azure_data_sources.storage_account
            }
          }

          dynamic "env" {
            for_each = var.cloud_provider == "gcp" && var.gcp_data_sources != null ? [1] : []
            content {
              name  = "GCP_STORAGE_BUCKET"
              value = var.gcp_data_sources.storage_bucket
            }
          }

          resources {
            limits = {
              cpu    = "1"
              memory = "2Gi"
            }
            requests = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }

          volume_mount {
            name       = "great-expectations-config"
            mount_path = "/app/great_expectations"
            read_only  = true
          }
        }

        volume {
          name = "great-expectations-config"
          config_map {
            name = kubernetes_config_map.data_quality_config[0].metadata[0].name
          }
        }
      }
    }
  }
}

# Data Lineage Tracker using Apache Airflow with OpenLineage
resource "helm_release" "airflow_lineage" {
  count = local.platform_components["lineage-tracker"] ? 1 : 0

  name       = "airflow-lineage"
  repository = "https://airflow.apache.org"
  chart      = "airflow"
  version    = "1.10.0"
  namespace  = kubernetes_namespace.data_platform.metadata[0].name

  values = [
    yamlencode({
      executor = "KubernetesExecutor"
      
      webserver = {
        replicas = var.env_config.high_availability ? 2 : 1
        
        service = {
          type = "ClusterIP"
        }
        
        resources = {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
          requests = {
            cpu    = "500m"
            memory = "1Gi"
          }
        }
      }
      
      scheduler = {
        replicas = var.env_config.high_availability ? 2 : 1
        
        resources = {
          limits = {
            cpu    = "2"
            memory = "4Gi"
          }
          requests = {
            cpu    = "1"
            memory = "2Gi"
          }
        }
      }
      
      # OpenLineage integration
      config = {
        openlineage = {
          transport = {
            type = "http"
            url  = "http://${kubernetes_service.lineage_backend[0].metadata[0].name}:8080"
          }
        }
      }
      
      # Data lineage DAGs
      dags = {
        persistence = {
          enabled      = true
          size         = "10Gi"
          storageClass = var.storage_class
        }
      }
      
      postgresql = {
        enabled = false  # Use external database
      }
      
      # External database configuration
      data = {
        metadataConnection = {
          user     = "airflow"
          pass     = "changeme"  # Should use secret
          host     = "postgresql"
          port     = 5432
          db       = "airflow"
          sslmode  = "require"
        }
      }
      
      # Security configuration
      webserverSecretKey = "changeme"  # Should use secret
      
      # Monitoring
      serviceMonitor = {
        enabled = var.env_config.monitoring_level != "basic"
      }
    })
  ]

  depends_on = [kubernetes_namespace.data_platform]
}

# Stream Processing with Apache Kafka and Hudi
resource "helm_release" "kafka_streams" {
  count = var.enable_streaming ? 1 : 0

  name       = "kafka-streams"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "kafka"
  version    = "22.1.5"
  namespace  = kubernetes_namespace.data_platform.metadata[0].name

  values = [
    yamlencode({
      replicaCount = var.env_config.high_availability ? 3 : 1
      
      # Auto-scaling configuration
      auth = {
        clientProtocol = "sasl"
        sasl = {
          mechanisms = ["SCRAM-SHA-256"]
        }
      }
      
      # Performance tuning
      config = "num.network.threads=8\nnum.io.threads=16\nsocket.send.buffer.bytes=102400\nsocket.receive.buffer.bytes=102400\nsocket.request.max.bytes=104857600\nnum.partitions=6\nnum.recovery.threads.per.data.dir=1\noffsets.topic.replication.factor=3\ntransaction.state.log.replication.factor=3\ntransaction.state.log.min.isr=2\nlog.retention.hours=168\nlog.segment.bytes=1073741824\nlog.retention.check.interval.ms=300000\ncompression.type=lz4"
      
      persistence = {
        enabled      = true
        size         = var.env_config.cluster_size == "large" ? "100Gi" : "50Gi"
        storageClass = var.storage_class
      }
      
      resources = {
        limits = var.env_config.cluster_size == "large" ? {
          cpu    = "2"
          memory = "4Gi"
        } : {
          cpu    = "1"
          memory = "2Gi"
        }
        requests = var.env_config.cluster_size == "large" ? {
          cpu    = "1"
          memory = "2Gi"
        } : {
          cpu    = "500m"
          memory = "1Gi"
        }
      }
      
      # Cost optimization with node affinity
      nodeSelector = {
        "node.kubernetes.io/instance-type" = var.env_config.cluster_size == "large" ? "m5.xlarge" : "t3.large"
      }
      
      # Zookeeper configuration
      zookeeper = {
        enabled      = true
        replicaCount = var.env_config.high_availability ? 3 : 1
        
        persistence = {
          enabled      = true
          size         = "10Gi"
          storageClass = var.storage_class
        }
      }
      
      # Monitoring and metrics
      metrics = {
        kafka = {
          enabled = var.env_config.monitoring_level != "basic"
        }
        jmx = {
          enabled = var.env_config.monitoring_level != "basic"
        }
      }
    })
  ]
}

# Namespace for data platform
resource "kubernetes_namespace" "data_platform" {
  metadata {
    name = "pwc-data-platform"
    
    labels = merge(var.common_tags, {
      component               = "data-platform"
      "istio-injection"      = "enabled"
      "pod-security.kubernetes.io/enforce" = "restricted"
    })
    
    annotations = {
      "kubectl.kubernetes.io/last-applied-configuration" = jsonencode({
        apiVersion = "v1"
        kind       = "Namespace"
        metadata = {
          name = "pwc-data-platform"
        }
      })
    }
  }
}

# Service Account for data platform components
resource "kubernetes_service_account" "data_platform" {
  metadata {
    name      = "data-platform-sa"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = var.common_tags
    
    annotations = {
      # Cloud-specific service account annotations
      "eks.amazonaws.com/role-arn"               = var.cloud_provider == "aws" ? var.aws_data_sources != null ? "arn:aws:iam::123456789012:role/data-platform-role" : null : null
      "azure.workload.identity/client-id"       = var.cloud_provider == "azure" ? "your-managed-identity-client-id" : null
      "iam.gke.io/gcp-service-account"         = var.cloud_provider == "gcp" ? "data-platform@project-id.iam.gserviceaccount.com" : null
    }
  }
  
  automount_service_account_token = true
}

# RBAC for data platform
resource "kubernetes_cluster_role" "data_platform" {
  metadata {
    name = "${var.resource_prefix}-data-platform"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services", "endpoints", "configmaps", "secrets", "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "replicasets", "statefulsets", "daemonsets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  
  rule {
    api_groups = ["batch"]
    resources  = ["jobs", "cronjobs"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
  
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications", "scheduledsparkapplications"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "data_platform" {
  metadata {
    name = "${var.resource_prefix}-data-platform"
  }
  
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.data_platform.metadata[0].name
  }
  
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.data_platform.metadata[0].name
    namespace = kubernetes_namespace.data_platform.metadata[0].name
  }
}

# Persistent Volume Claims for data storage
resource "kubernetes_persistent_volume_claim" "catalog_storage" {
  count = local.platform_components["data-catalog"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-catalog-storage"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = var.common_tags
  }

  spec {
    access_modes = ["ReadWriteOnce"]
    
    resources {
      requests = {
        storage = var.env_config.cluster_size == "large" ? "100Gi" : "50Gi"
      }
    }
    
    storage_class_name = var.storage_class
  }
}

# ConfigMaps for component configurations
resource "kubernetes_config_map" "data_catalog_config" {
  count = local.platform_components["data-catalog"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-catalog-config"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = var.common_tags
  }

  data = {
    "atlas-application.properties" = templatefile("${path.module}/configs/atlas-application.properties", {
      database_url = var.database_url
      kafka_bootstrap_servers = var.enable_streaming ? "kafka-streams:9092" : ""
    })
    
    "atlas-log4j.xml" = file("${path.module}/configs/atlas-log4j.xml")
  }
}

resource "kubernetes_config_map" "data_quality_config" {
  count = local.platform_components["quality-monitor"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-data-quality-config"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = var.common_tags
  }

  data = {
    "great_expectations.yml" = templatefile("${path.module}/configs/great_expectations.yml", {
      cloud_provider = var.cloud_provider
      database_url   = var.database_url
      s3_bucket      = var.cloud_provider == "aws" && var.aws_data_sources != null ? var.aws_data_sources.s3_bucket : ""
      storage_account = var.cloud_provider == "azure" && var.azure_data_sources != null ? var.azure_data_sources.storage_account : ""
      gcs_bucket     = var.cloud_provider == "gcp" && var.gcp_data_sources != null ? var.gcp_data_sources.storage_bucket : ""
    })
  }
}

# Secrets for sensitive configuration
resource "kubernetes_secret" "data_platform_secrets" {
  metadata {
    name      = "${var.resource_prefix}-secrets"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = var.common_tags
  }

  type = "Opaque"

  data = {
    "database-url"         = base64encode(var.database_url)
    "catalog-database-url" = base64encode("${var.database_url}_catalog")
    
    # Cloud-specific credentials
    "aws-access-key"    = var.cloud_provider == "aws" ? base64encode(var.aws_credentials.access_key) : ""
    "aws-secret-key"    = var.cloud_provider == "aws" ? base64encode(var.aws_credentials.secret_key) : ""
    "azure-tenant-id"   = var.cloud_provider == "azure" ? base64encode(var.azure_credentials.tenant_id) : ""
    "azure-client-id"   = var.cloud_provider == "azure" ? base64encode(var.azure_credentials.client_id) : ""
    "gcp-service-key"   = var.cloud_provider == "gcp" ? base64encode(var.gcp_credentials.service_account_key) : ""
  }
}

# Service for data lineage backend
resource "kubernetes_service" "lineage_backend" {
  count = local.platform_components["lineage-tracker"] ? 1 : 0

  metadata {
    name      = "${var.resource_prefix}-lineage-backend"
    namespace = kubernetes_namespace.data_platform.metadata[0].name
    
    labels = merge(var.common_tags, {
      component = "lineage-backend"
    })
  }

  spec {
    selector = {
      app = "${var.resource_prefix}-lineage-backend"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }

    type = "ClusterIP"
  }
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
}

variable "cloud_provider" {
  description = "Cloud provider (aws, azure, gcp, multi)"
  type        = string
}

variable "resource_prefix" {
  description = "Resource naming prefix"
  type        = string
}

variable "common_tags" {
  description = "Common tags for all resources"
  type        = map(string)
}

variable "env_config" {
  description = "Environment-specific configuration"
  type = object({
    cluster_size        = string
    high_availability  = bool
    auto_scaling       = bool
    backup_retention   = number
    monitoring_level   = string
  })
}

variable "enable_disaster_recovery" {
  description = "Enable disaster recovery features"
  type        = bool
  default     = false
}

variable "enable_streaming" {
  description = "Enable streaming data processing"
  type        = bool
  default     = true
}

variable "enable_spot_instances" {
  description = "Enable spot instances for cost optimization"
  type        = bool
  default     = false
}

variable "storage_class" {
  description = "Kubernetes storage class"
  type        = string
  default     = "gp2"
}

variable "database_url" {
  description = "Database connection URL"
  type        = string
  sensitive   = true
}

# Cloud-specific data sources
variable "aws_data_sources" {
  description = "AWS data sources configuration"
  type = object({
    s3_bucket           = string
    redshift_cluster    = string
    kinesis_streams     = map(string)
  })
  default = null
}

variable "azure_data_sources" {
  description = "Azure data sources configuration"
  type = object({
    storage_account     = string
    synapse_workspace   = string
    event_hubs          = map(string)
  })
  default = null
}

variable "gcp_data_sources" {
  description = "GCP data sources configuration"
  type = object({
    storage_bucket      = string
    bigquery_dataset    = string
    pub_sub_topics      = map(string)
  })
  default = null
}

# Cloud credentials (should be managed via external secret management)
variable "aws_credentials" {
  description = "AWS credentials"
  type = object({
    access_key = string
    secret_key = string
  })
  default = {
    access_key = ""
    secret_key = ""
  }
  sensitive = true
}

variable "azure_credentials" {
  description = "Azure credentials"
  type = object({
    tenant_id   = string
    client_id   = string
  })
  default = {
    tenant_id   = ""
    client_id   = ""
  }
  sensitive = true
}

variable "gcp_credentials" {
  description = "GCP credentials"
  type = object({
    service_account_key = string
  })
  default = {
    service_account_key = ""
  }
  sensitive = true
}

# Outputs
output "namespace" {
  description = "Data platform namespace"
  value       = kubernetes_namespace.data_platform.metadata[0].name
}

output "service_account" {
  description = "Data platform service account"
  value       = kubernetes_service_account.data_platform.metadata[0].name
}

output "data_catalog_service" {
  description = "Data catalog service endpoint"
  value = local.platform_components["data-catalog"] ? "http://${var.resource_prefix}-data-catalog.${kubernetes_namespace.data_platform.metadata[0].name}.svc.cluster.local:21000" : null
}

output "spark_operator_namespace" {
  description = "Spark operator namespace"
  value = local.platform_components["spark-operator"] ? "spark-system" : null
}

output "platform_components" {
  description = "Enabled platform components"
  value       = local.platform_components
}