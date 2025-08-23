# Kubernetes Applications Module for PwC Retail Data Platform
# Deploys all application components to Kubernetes cluster

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

# ============================================================================
# NAMESPACE AND RBAC
# ============================================================================

# Create namespace for the application
resource "kubernetes_namespace" "main" {
  metadata {
    name = var.namespace
    labels = merge(var.labels, {
      name        = var.namespace
      environment = var.environment
    })
  }
}

# Service Account for applications
resource "kubernetes_service_account" "app" {
  metadata {
    name      = "${var.project_name}-service-account"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
}

# ============================================================================
# SECRETS AND CONFIGMAPS
# ============================================================================

# Database connection secret
resource "kubernetes_secret" "database" {
  metadata {
    name      = "${var.project_name}-database-secret"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  type = "Opaque"
  
  data = {
    host     = var.database_host
    port     = tostring(5432)
    database = var.database_name
    username = var.database_username
    password = var.database_password
    url      = "postgresql://${var.database_username}:${var.database_password}@${var.database_host}:5432/${var.database_name}"
  }
}

# Application configuration
resource "kubernetes_config_map" "app_config" {
  metadata {
    name      = "${var.project_name}-config"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  data = {
    ENVIRONMENT           = var.environment
    DATABASE_TYPE        = "postgresql"
    ENABLE_MONITORING    = tostring(var.enable_monitoring)
    ENABLE_LOGGING       = tostring(var.enable_logging)
    API_PORT            = "8000"
    DAGSTER_PORT        = "3000"
    PROMETHEUS_PORT     = "9090"
    GRAFANA_PORT        = "3001"
    LOG_LEVEL          = var.environment == "prod" ? "INFO" : "DEBUG"
    PYTHONPATH         = "src"
  }
}

# ============================================================================
# PERSISTENT VOLUMES
# ============================================================================

# Storage Class for high-performance SSD (cloud provider specific)
resource "kubernetes_storage_class" "fast_ssd" {
  metadata {
    name = "${var.project_name}-fast-ssd"
    labels = var.labels
  }
  
  storage_provisioner    = var.storage_provisioner
  reclaim_policy        = "Retain"
  volume_binding_mode   = "WaitForFirstConsumer"
  allow_volume_expansion = true
  
  parameters = var.storage_parameters
}

# Persistent Volume Claim for data storage
resource "kubernetes_persistent_volume_claim" "data_storage" {
  metadata {
    name      = "${var.project_name}-data-pvc"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    access_modes       = ["ReadWriteOnce"]
    storage_class_name = kubernetes_storage_class.fast_ssd.metadata[0].name
    
    resources {
      requests = {
        storage = "100Gi"
      }
    }
  }
}

# ============================================================================
# API SERVICE
# ============================================================================

# API Deployment
resource "kubernetes_deployment" "api" {
  metadata {
    name      = "${var.project_name}-api"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "api"
      version   = "v1"
    })
  }
  
  spec {
    replicas = var.environment == "prod" ? 3 : 2
    
    selector {
      match_labels = {
        app       = "${var.project_name}-api"
        component = "api"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-api"
          component = "api"
          version   = "v1"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8000"
          "prometheus.io/path"   = "/api/v1/metrics"
        }
      }
      
      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        
        security_context {
          run_as_non_root = true
          run_as_user     = 65534
          fs_group        = 65534
        }
        
        container {
          name  = "api"
          image = var.api_image
          
          port {
            name           = "http"
            container_port = 8000
            protocol       = "TCP"
          }
          
          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = true
            run_as_non_root           = true
            run_as_user               = 65534
            capabilities {
              drop = ["ALL"]
            }
          }
          
          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }
          
          env_from {
            secret_ref {
              name = kubernetes_secret.database.metadata[0].name
            }
          }
          
          env {
            name  = "DATABASE_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.database.metadata[0].name
                key  = "url"
              }
            }
          }
          
          resources {
            requests = {
              cpu    = var.api_cpu_request
              memory = var.api_memory_request
            }
            limits = {
              cpu    = "1000m"
              memory = "1Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/api/v1/health"
              port = 8000
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 3
          }
          
          readiness_probe {
            http_get {
              path = "/api/v1/health"
              port = 8000
            }
            initial_delay_seconds = 10
            period_seconds        = 5
            timeout_seconds       = 3
            failure_threshold     = 3
          }
          
          volume_mount {
            name       = "data-storage"
            mount_path = "/app/data"
          }
        }
        
        volume {
          name = "data-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.data_storage.metadata[0].name
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# API Service
resource "kubernetes_service" "api" {
  metadata {
    name      = "${var.project_name}-api-service"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "api"
    })
    annotations = var.service_annotations
  }
  
  spec {
    selector = {
      app       = "${var.project_name}-api"
      component = "api"
    }
    
    port {
      name        = "http"
      port        = 80
      target_port = 8000
      protocol    = "TCP"
    }
    
    type = "LoadBalancer"
  }
}

# ============================================================================
# ETL SERVICE
# ============================================================================

# ETL Deployment
resource "kubernetes_deployment" "etl" {
  metadata {
    name      = "${var.project_name}-etl"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "etl"
      version   = "v1"
    })
  }
  
  spec {
    replicas = 1  # ETL jobs typically run as single instances
    
    selector {
      match_labels = {
        app       = "${var.project_name}-etl"
        component = "etl"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-etl"
          component = "etl"
          version   = "v1"
        })
      }
      
      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        
        security_context {
          run_as_non_root = true
          run_as_user     = 65534
          fs_group        = 65534
        }
        
        container {
          name  = "etl"
          image = var.etl_image
          
          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = false  # ETL needs write access
            run_as_non_root           = true
            run_as_user               = 65534
            capabilities {
              drop = ["ALL"]
            }
          }
          
          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }
          
          env_from {
            secret_ref {
              name = kubernetes_secret.database.metadata[0].name
            }
          }
          
          env {
            name  = "PROCESSING_ENGINE"
            value = "spark"
          }
          
          env {
            name  = "SPARK_MASTER"
            value = "local[*]"
          }
          
          resources {
            requests = {
              cpu    = var.etl_cpu_request
              memory = var.etl_memory_request
            }
            limits = {
              cpu    = "4000m"
              memory = "8Gi"
            }
          }
          
          volume_mount {
            name       = "data-storage"
            mount_path = "/app/data"
          }
        }
        
        volume {
          name = "data-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.data_storage.metadata[0].name
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# ============================================================================
# DBT SERVICE
# ============================================================================

# dbt Job for data transformations
resource "kubernetes_job" "dbt_run" {
  metadata {
    name      = "${var.project_name}-dbt-run"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "dbt"
      job-type  = "transformation"
    })
  }
  
  spec {
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-dbt"
          component = "dbt"
        })
      }
      
      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        restart_policy      = "OnFailure"
        
        container {
          name  = "dbt"
          image = var.dbt_image
          
          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }
          
          env_from {
            secret_ref {
              name = kubernetes_secret.database.metadata[0].name
            }
          }
          
          env {
            name  = "DBT_TARGET"
            value = var.environment
          }
          
          env {
            name  = "DBT_PROFILES_DIR"
            value = "/app"
          }
          
          command = ["dbt"]
          args    = ["run", "--profiles-dir", "/app", "--target", var.environment]
          
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
        }
      }
    }
    
    backoff_limit = 3
  }
}

# dbt CronJob for scheduled runs
resource "kubernetes_cron_job_v1" "dbt_scheduled" {
  metadata {
    name      = "${var.project_name}-dbt-scheduled"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "dbt"
      job-type  = "scheduled"
    })
  }
  
  spec {
    schedule          = "0 2 * * *"  # Daily at 2 AM
    concurrency_policy = "Forbid"
    
    job_template {
      metadata {
        labels = merge(var.labels, {
          component = "dbt"
          job-type  = "scheduled"
        })
      }
      
      spec {
        template {
          metadata {
            labels = merge(var.labels, {
              app       = "${var.project_name}-dbt"
              component = "dbt"
            })
          }
          
          spec {
            service_account_name = kubernetes_service_account.app.metadata[0].name
            restart_policy      = "OnFailure"
            
            container {
              name  = "dbt"
              image = var.dbt_image
              
              env_from {
                config_map_ref {
                  name = kubernetes_config_map.app_config.metadata[0].name
                }
              }
              
              env_from {
                secret_ref {
                  name = kubernetes_secret.database.metadata[0].name
                }
              }
              
              env {
                name  = "DBT_TARGET"
                value = var.environment
              }
              
              command = ["sh", "-c"]
              args = [
                "dbt run --profiles-dir /app --target ${var.environment} && dbt test --profiles-dir /app --target ${var.environment}"
              ]
              
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
            }
          }
        }
        
        backoff_limit = 2
      }
    }
  }
}

# ============================================================================
# DAGSTER ORCHESTRATION
# ============================================================================

# Dagster Deployment
resource "kubernetes_deployment" "dagster" {
  metadata {
    name      = "${var.project_name}-dagster"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "dagster"
      version   = "v1"
    })
  }
  
  spec {
    replicas = 1
    
    selector {
      match_labels = {
        app       = "${var.project_name}-dagster"
        component = "dagster"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-dagster"
          component = "dagster"
          version   = "v1"
        })
      }
      
      spec {
        service_account_name = kubernetes_service_account.app.metadata[0].name
        
        security_context {
          run_as_non_root = true
          run_as_user     = 65534
          fs_group        = 65534
        }
        
        container {
          name  = "dagster"
          image = var.dagster_image
          
          security_context {
            allow_privilege_escalation = false
            read_only_root_filesystem  = false  # Dagster needs write access for logs
            run_as_non_root           = true
            run_as_user               = 65534
            capabilities {
              drop = ["ALL"]
            }
          }
          
          port {
            name           = "http"
            container_port = 3000
            protocol       = "TCP"
          }
          
          env_from {
            config_map_ref {
              name = kubernetes_config_map.app_config.metadata[0].name
            }
          }
          
          env_from {
            secret_ref {
              name = kubernetes_secret.database.metadata[0].name
            }
          }
          
          env {
            name  = "DAGSTER_HOME"
            value = "/app/dagster_home"
          }
          
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
          
          liveness_probe {
            http_get {
              path = "/server_info"
              port = 3000
            }
            initial_delay_seconds = 60
            period_seconds        = 30
            timeout_seconds       = 10
            failure_threshold     = 3
          }
          
          readiness_probe {
            http_get {
              path = "/server_info"
              port = 3000
            }
            initial_delay_seconds = 30
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 3
          }
          
          volume_mount {
            name       = "data-storage"
            mount_path = "/app/data"
          }
        }
        
        volume {
          name = "data-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.data_storage.metadata[0].name
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# Dagster Service
resource "kubernetes_service" "dagster" {
  metadata {
    name      = "${var.project_name}-dagster-service"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels = merge(var.labels, {
      component = "dagster"
    })
  }
  
  spec {
    selector = {
      app       = "${var.project_name}-dagster"
      component = "dagster"
    }
    
    port {
      name        = "http"
      port        = 3000
      target_port = 3000
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# ============================================================================
# HORIZONTAL POD AUTOSCALER
# ============================================================================

# HPA for API
resource "kubernetes_horizontal_pod_autoscaler_v2" "api" {
  metadata {
    name      = "${var.project_name}-api-hpa"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment.api.metadata[0].name
    }
    
    min_replicas = var.environment == "prod" ? 2 : 1
    max_replicas = var.environment == "prod" ? 10 : 5
    
    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = 70
        }
      }
    }
    
    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = 80
        }
      }
    }
    
    behavior {
      scale_up {
        stabilization_window_seconds = 60
        policy {
          type          = "Percent"
          value         = 100
          period_seconds = 15
        }
      }
      
      scale_down {
        stabilization_window_seconds = 300
        policy {
          type          = "Percent"
          value         = 50
          period_seconds = 60
        }
      }
    }
  }
}

# ============================================================================
# NETWORK POLICIES
# ============================================================================

# Network policy for API pods
resource "kubernetes_network_policy" "api_network_policy" {
  metadata {
    name      = "${var.project_name}-api-network-policy"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    pod_selector {
      match_labels = {
        component = "api"
      }
    }
    
    policy_types = ["Ingress", "Egress"]
    
    ingress {
      from {
        namespace_selector {
          match_labels = {
            name = "kube-system"
          }
        }
      }
      from {
        pod_selector {
          match_labels = {
            component = "dagster"
          }
        }
      }
      ports {
        port     = "8000"
        protocol = "TCP"
      }
    }
    
    egress {
      # Allow DNS resolution
      to {
        namespace_selector {
          match_labels = {
            name = "kube-system"
          }
        }
      }
      ports {
        port     = "53"
        protocol = "UDP"
      }
    }
    
    egress {
      # Allow database connections
      ports {
        port     = "5432"
        protocol = "TCP"
      }
    }
  }
}

# Network policy for ETL pods
resource "kubernetes_network_policy" "etl_network_policy" {
  metadata {
    name      = "${var.project_name}-etl-network-policy"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    pod_selector {
      match_labels = {
        component = "etl"
      }
    }
    
    policy_types = ["Ingress", "Egress"]
    
    ingress {
      from {
        pod_selector {
          match_labels = {
            component = "dagster"
          }
        }
      }
    }
    
    egress {
      # Allow DNS resolution
      to {
        namespace_selector {
          match_labels = {
            name = "kube-system"
          }
        }
      }
      ports {
        port     = "53"
        protocol = "UDP"
      }
    }
    
    egress {
      # Allow database connections
      ports {
        port     = "5432"
        protocol = "TCP"
      }
    }
  }
}

# ============================================================================
# RESOURCE QUOTAS AND LIMITS
# ============================================================================

# Resource quota for the namespace
resource "kubernetes_resource_quota" "main" {
  metadata {
    name      = "${var.project_name}-resource-quota"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    hard = {
      "requests.cpu"    = var.environment == "prod" ? "8" : "4"
      "requests.memory" = var.environment == "prod" ? "16Gi" : "8Gi"
      "limits.cpu"      = var.environment == "prod" ? "16" : "8"
      "limits.memory"   = var.environment == "prod" ? "32Gi" : "16Gi"
      "pods"           = var.environment == "prod" ? "20" : "10"
      "services"       = "10"
      "persistentvolumeclaims" = "5"
      "secrets"        = "10"
      "configmaps"     = "10"
    }
  }
}

# Limit range for default resource limits
resource "kubernetes_limit_range" "main" {
  metadata {
    name      = "${var.project_name}-limit-range"
    namespace = kubernetes_namespace.main.metadata[0].name
    labels    = var.labels
  }
  
  spec {
    limit {
      type = "Container"
      default = {
        cpu    = "500m"
        memory = "1Gi"
      }
      default_request = {
        cpu    = "100m"
        memory = "256Mi"
      }
      max = {
        cpu    = "4"
        memory = "8Gi"
      }
    }
    
    limit {
      type = "Pod"
      max = {
        cpu    = "8"
        memory = "16Gi"
      }
    }
  }
}