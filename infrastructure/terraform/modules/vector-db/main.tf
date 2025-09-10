# BMAD Platform - Vector Database Infrastructure for AI Story 4.2
# High-performance vector search with embedding generation and semantic similarity

terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
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
  }
}

locals {
  common_labels = var.tags
  
  # Vector database configuration
  vector_config = {
    dimension          = var.vector_dimension
    index_type        = var.index_type
    similarity_metric = var.similarity_metric
    max_connections   = var.max_connections
    storage_size      = var.storage_size
    storage_class     = var.storage_class
  }
  
  # AI model configurations
  ai_models = {
    openai_embeddings = {
      model_name = "text-embedding-ada-002"
      dimensions = 1536
      max_tokens = 8191
    }
    sentence_transformers = {
      model_name = "all-MiniLM-L6-v2"
      dimensions = 384
      max_tokens = 512
    }
    cohere_embeddings = {
      model_name = "embed-english-v3.0"
      dimensions = 1024
      max_tokens = 512
    }
  }
  
  # Performance optimization settings
  performance_config = {
    index_build_threads = var.index_build_threads
    search_threads     = var.search_threads
    memory_limit       = "16Gi"
    cpu_limit         = "8000m"
    replica_count     = 3
  }
}

# Namespace for Vector Database
resource "kubernetes_namespace" "vector_db" {
  metadata {
    name = "vector-db"
    
    labels = merge(local.common_labels, {
      purpose = "vector-database"
      story   = "4.2"
    })
  }
}

# Pinecone Vector Database Deployment (Primary)
resource "helm_release" "pinecone_operator" {
  count = var.deploy_pinecone ? 1 : 0
  
  name       = "pinecone-operator"
  repository = "https://pinecone-io.github.io/pinecone-kubernetes-operator"
  chart      = "pinecone-operator"
  version    = "1.0.0"
  namespace  = kubernetes_namespace.vector_db.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/pinecone-values.yaml", {
      api_key    = var.pinecone_api_key
      environment = var.pinecone_environment
      dimension  = local.vector_config.dimension
      metric     = local.vector_config.similarity_metric
      replicas   = local.performance_config.replica_count
    })
  ]
  
  depends_on = [kubernetes_namespace.vector_db]
}

# Weaviate Vector Database (Alternative/Backup)
resource "helm_release" "weaviate" {
  count = var.deploy_weaviate ? 1 : 0
  
  name       = "weaviate"
  repository = "https://weaviate.github.io/weaviate-helm"
  chart      = "weaviate"
  version    = "16.8.2"
  namespace  = kubernetes_namespace.vector_db.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/weaviate-values.yaml", {
      replicas        = local.performance_config.replica_count
      storage_size    = local.vector_config.storage_size
      storage_class   = local.vector_config.storage_class
      memory_limit    = local.performance_config.memory_limit
      cpu_limit      = local.performance_config.cpu_limit
      enable_backup  = var.backup_enabled
      enable_auth    = var.enable_auth
      enable_tls     = var.enable_tls
    })
  ]
  
  depends_on = [kubernetes_namespace.vector_db]
}

# Qdrant Vector Database (High Performance)
resource "kubernetes_deployment" "qdrant" {
  count = var.deploy_qdrant ? 1 : 0
  
  metadata {
    name      = "qdrant"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app     = "qdrant"
      version = "v1.6.1"
    }
  }

  spec {
    replicas = local.performance_config.replica_count

    selector {
      match_labels = {
        app = "qdrant"
      }
    }

    template {
      metadata {
        labels = {
          app = "qdrant"
        }
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "6333"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        container {
          image = "qdrant/qdrant:v1.6.1"
          name  = "qdrant"
          
          port {
            container_port = 6333
            name          = "http"
          }
          
          port {
            container_port = 6334
            name          = "grpc"
          }
          
          env {
            name  = "QDRANT__SERVICE__HOST"
            value = "0.0.0.0"
          }
          
          env {
            name  = "QDRANT__SERVICE__HTTP_PORT"
            value = "6333"
          }
          
          env {
            name  = "QDRANT__SERVICE__GRPC_PORT"
            value = "6334"
          }
          
          env {
            name  = "QDRANT__LOG_LEVEL"
            value = "INFO"
          }
          
          env {
            name  = "QDRANT__STORAGE__PERFORMANCE__MAX_SEARCH_THREADS"
            value = tostring(local.performance_config.search_threads)
          }
          
          env {
            name  = "QDRANT__STORAGE__PERFORMANCE__MAX_OPTIMIZATION_THREADS"
            value = tostring(local.performance_config.index_build_threads)
          }
          
          resources {
            requests = {
              cpu    = "1000m"
              memory = "4Gi"
            }
            limits = {
              cpu    = local.performance_config.cpu_limit
              memory = local.performance_config.memory_limit
            }
          }
          
          volume_mount {
            name       = "qdrant-storage"
            mount_path = "/qdrant/storage"
          }
          
          liveness_probe {
            http_get {
              path = "/health"
              port = 6333
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/health"
              port = 6333
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
        }
        
        volume {
          name = "qdrant-storage"
          persistent_volume_claim {
            claim_name = kubernetes_persistent_volume_claim.qdrant_storage[0].metadata[0].name
          }
        }
        
        # Anti-affinity for high availability
        affinity {
          pod_anti_affinity {
            preferred_during_scheduling_ignored_during_execution {
              weight = 100
              pod_affinity_term {
                label_selector {
                  match_expressions {
                    key      = "app"
                    operator = "In"
                    values   = ["qdrant"]
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

# Persistent Volume Claim for Qdrant
resource "kubernetes_persistent_volume_claim" "qdrant_storage" {
  count = var.deploy_qdrant ? 1 : 0
  
  metadata {
    name      = "qdrant-storage"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
  }
  
  spec {
    access_modes = ["ReadWriteOnce"]
    
    resources {
      requests = {
        storage = local.vector_config.storage_size
      }
    }
    
    storage_class_name = local.vector_config.storage_class
  }
}

# Qdrant Service
resource "kubernetes_service" "qdrant" {
  count = var.deploy_qdrant ? 1 : 0
  
  metadata {
    name      = "qdrant-service"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app = "qdrant"
    }
    
    annotations = {
      "service.beta.kubernetes.io/aws-load-balancer-type" = "nlb"
    }
  }
  
  spec {
    selector = {
      app = "qdrant"
    }
    
    port {
      name        = "http"
      port        = 6333
      target_port = 6333
      protocol    = "TCP"
    }
    
    port {
      name        = "grpc"
      port        = 6334
      target_port = 6334
      protocol    = "TCP"
    }
    
    type = "LoadBalancer"
  }
}

# Embedding Generation Service
resource "kubernetes_deployment" "embedding_service" {
  metadata {
    name      = "embedding-service"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app     = "embedding-service"
      version = "v1.0.0"
    }
  }

  spec {
    replicas = 3

    selector {
      match_labels = {
        app = "embedding-service"
      }
    }

    template {
      metadata {
        labels = {
          app = "embedding-service"
        }
      }

      spec {
        container {
          image = "bmad/embedding-service:v1.0.0"
          name  = "embedding-service"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name = "OPENAI_API_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.ai_api_keys.metadata[0].name
                key  = "openai-api-key"
              }
            }
          }
          
          env {
            name = "COHERE_API_KEY"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.ai_api_keys.metadata[0].name
                key  = "cohere-api-key"
              }
            }
          }
          
          env {
            name  = "VECTOR_DB_ENDPOINT"
            value = var.deploy_qdrant ? "http://qdrant-service.vector-db.svc.cluster.local:6333" : "http://weaviate.vector-db.svc.cluster.local:8080"
          }
          
          env {
            name  = "EMBEDDING_MODEL"
            value = "text-embedding-ada-002"
          }
          
          env {
            name  = "BATCH_SIZE"
            value = "100"
          }
          
          env {
            name  = "MAX_CONCURRENT_REQUESTS"
            value = "10"
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
          }
          
          readiness_probe {
            http_get {
              path = "/ready"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds        = 5
          }
        }
      }
    }
  }
}

# Semantic Search API Service
resource "kubernetes_deployment" "semantic_search_api" {
  metadata {
    name      = "semantic-search-api"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app     = "semantic-search-api"
      version = "v1.0.0"
    }
  }

  spec {
    replicas = 5

    selector {
      match_labels = {
        app = "semantic-search-api"
      }
    }

    template {
      metadata {
        labels = {
          app = "semantic-search-api"
        }
      }

      spec {
        container {
          image = "bmad/semantic-search-api:v1.0.0"
          name  = "semantic-search-api"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name  = "VECTOR_DB_ENDPOINT"
            value = var.deploy_qdrant ? "http://qdrant-service.vector-db.svc.cluster.local:6333" : "http://weaviate.vector-db.svc.cluster.local:8080"
          }
          
          env {
            name  = "EMBEDDING_SERVICE_ENDPOINT"
            value = "http://embedding-service.vector-db.svc.cluster.local:8080"
          }
          
          env {
            name  = "DEFAULT_TOP_K"
            value = "10"
          }
          
          env {
            name  = "DEFAULT_SIMILARITY_THRESHOLD"
            value = "0.7"
          }
          
          env {
            name  = "CACHE_TTL_SECONDS"
            value = "300"
          }
          
          env {
            name  = "MAX_QUERY_LENGTH"
            value = "1000"
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
        }
      }
    }
  }
}

# API Keys Secret
resource "kubernetes_secret" "ai_api_keys" {
  metadata {
    name      = "ai-api-keys"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
  }
  
  data = {
    openai-api-key = var.openai_api_key
    cohere-api-key = var.cohere_api_key
    pinecone-api-key = var.pinecone_api_key
  }
  
  type = "Opaque"
}

# Vector Search Cache (Redis)
resource "helm_release" "redis_cache" {
  name       = "redis-vector-cache"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "redis"
  version    = "18.1.5"
  namespace  = kubernetes_namespace.vector_db.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/redis-cache-values.yaml", {
      memory_limit = "4Gi"
      persistence_size = "20Gi"
      replicas = 3
    })
  ]
  
  depends_on = [kubernetes_namespace.vector_db]
}

# Vector Database Monitoring
resource "kubernetes_service_monitor" "vector_db_monitoring" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "vector-db-monitoring"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app = "vector-db-monitoring"
    }
  }
  
  spec {
    selector {
      match_labels = {
        app = "qdrant"
      }
    }
    
    endpoints {
      port = "http"
      path = "/metrics"
      interval = "30s"
    }
  }
}

# Vector Database Backup Job
resource "kubernetes_cron_job" "vector_db_backup" {
  count = var.backup_enabled ? 1 : 0
  
  metadata {
    name      = "vector-db-backup"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
  }
  
  spec {
    concurrency_policy = "Forbid"
    schedule          = "0 2 * * *"  # Daily at 2 AM
    
    job_template {
      metadata {
        name = "vector-db-backup"
      }
      
      spec {
        template {
          metadata {
            name = "vector-db-backup"
          }
          
          spec {
            restart_policy = "OnFailure"
            
            container {
              name  = "backup"
              image = "bmad/vector-db-backup:v1.0.0"
              
              env {
                name  = "VECTOR_DB_ENDPOINT"
                value = var.deploy_qdrant ? "http://qdrant-service.vector-db.svc.cluster.local:6333" : "http://weaviate.vector-db.svc.cluster.local:8080"
              }
              
              env {
                name  = "BACKUP_DESTINATION"
                value = "s3://bmad-vector-db-backups/"
              }
              
              env {
                name = "AWS_ACCESS_KEY_ID"
                value_from {
                  secret_key_ref {
                    name = "aws-backup-credentials"
                    key  = "access-key-id"
                  }
                }
              }
              
              env {
                name = "AWS_SECRET_ACCESS_KEY"
                value_from {
                  secret_key_ref {
                    name = "aws-backup-credentials"
                    key  = "secret-access-key"
                  }
                }
              }
              
              resources {
                requests = {
                  cpu    = "100m"
                  memory = "256Mi"
                }
                limits = {
                  cpu    = "500m"
                  memory = "1Gi"
                }
              }
            }
          }
        }
      }
    }
  }
}

# Horizontal Pod Autoscaler for Semantic Search API
resource "kubernetes_horizontal_pod_autoscaler_v2" "semantic_search_hpa" {
  metadata {
    name      = "semantic-search-hpa"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
  }
  
  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = kubernetes_deployment.semantic_search_api.metadata[0].name
    }
    
    min_replicas = 3
    max_replicas = 20
    
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
    
    # Custom metric for vector search queries per second
    metric {
      type = "Pods"
      pods {
        metric {
          name = "vector_search_queries_per_second"
        }
        target {
          type          = "AverageValue"
          average_value = "500"
        }
      }
    }
  }
}

# Service for Embedding Service
resource "kubernetes_service" "embedding_service" {
  metadata {
    name      = "embedding-service"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app = "embedding-service"
    }
  }
  
  spec {
    selector = {
      app = "embedding-service"
    }
    
    port {
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# Service for Semantic Search API
resource "kubernetes_service" "semantic_search_api" {
  metadata {
    name      = "semantic-search-api"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    labels = {
      app = "semantic-search-api"
    }
    
    annotations = {
      "service.beta.kubernetes.io/aws-load-balancer-type" = "nlb"
    }
  }
  
  spec {
    selector = {
      app = "semantic-search-api"
    }
    
    port {
      port        = 80
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "LoadBalancer"
  }
}

# Ingress for Semantic Search API
resource "kubernetes_ingress_v1" "semantic_search_ingress" {
  metadata {
    name      = "semantic-search-ingress"
    namespace = kubernetes_namespace.vector_db.metadata[0].name
    
    annotations = {
      "kubernetes.io/ingress.class"                = "nginx"
      "nginx.ingress.kubernetes.io/ssl-redirect"   = "true"
      "nginx.ingress.kubernetes.io/rate-limit"     = "1000"
      "nginx.ingress.kubernetes.io/rate-limit-rps" = "100"
      "cert-manager.io/cluster-issuer"             = "letsencrypt-prod"
    }
  }
  
  spec {
    tls {
      hosts = ["ai.${var.domain_name}"]
      secret_name = "semantic-search-tls"
    }
    
    rule {
      host = "ai.${var.domain_name}"
      
      http {
        path {
          path      = "/search"
          path_type = "Prefix"
          
          backend {
            service {
              name = kubernetes_service.semantic_search_api.metadata[0].name
              port {
                number = 80
              }
            }
          }
        }
        
        path {
          path      = "/embed"
          path_type = "Prefix"
          
          backend {
            service {
              name = kubernetes_service.embedding_service.metadata[0].name
              port {
                number = 8080
              }
            }
          }
        }
      }
    }
  }
}