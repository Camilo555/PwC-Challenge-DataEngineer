# Vertical Pod Autoscaler (VPA) for Optimal Resource Allocation
# Production-ready VPA configuration with intelligent resource optimization

# ============================================================================
# VPA ADMISSION CONTROLLER CONFIGURATION
# ============================================================================

# VPA admission controller webhook configuration
resource "kubernetes_mutating_webhook_configuration_v1" "vpa_webhook" {
  metadata {
    name = "${var.project_name}-vpa-webhook"
    labels = merge(var.labels, {
      component = "vpa"
      tier      = "admission-controller"
    })
  }
  
  webhook {
    name = "vpa-webhook.k8s.io"
    
    client_config {
      service {
        name      = "vpa-webhook"
        namespace = var.namespace
        path      = "/webhook"
      }
    }
    
    admission_review_versions = ["v1", "v1beta1"]
    side_effects             = "None"
    failure_policy           = "Ignore"
    
    rule {
      operations   = ["CREATE", "UPDATE"]
      api_groups   = [""]
      api_versions = ["v1"]
      resources    = ["pods"]
    }
    
    rule {
      operations   = ["CREATE", "UPDATE"]
      api_groups   = ["apps"]
      api_versions = ["v1"]
      resources    = ["deployments", "daemonsets", "statefulsets", "replicasets"]
    }
    
    namespace_selector {
      match_expressions {
        key      = "vpa.k8s.io/enabled"
        operator = "In"
        values   = ["true"]
      }
    }
  }
}

# ============================================================================
# VPA FOR API SERVICE WITH INTELLIGENT RESOURCE OPTIMIZATION
# ============================================================================

resource "kubernetes_manifest" "vpa_api" {
  manifest = {
    apiVersion = "autoscaling.k8s.io/v1"
    kind       = "VerticalPodAutoscaler"
    metadata = {
      name      = "${var.project_name}-api-vpa"
      namespace = var.namespace
      labels = merge(var.labels, {
        component = "vpa"
        service   = "api"
        tier      = "intelligent"
      })
      annotations = {
        "vpa.k8s.io/update-mode"                = var.environment == "prod" ? "Auto" : "Off"
        "vpa.k8s.io/resource-policy"           = "optimized"
        "vpa.k8s.io/min-allowed-cpu"          = var.environment == "prod" ? "100m" : "50m"
        "vpa.k8s.io/min-allowed-memory"       = var.environment == "prod" ? "256Mi" : "128Mi"
        "vpa.k8s.io/max-allowed-cpu"          = var.environment == "prod" ? "8000m" : "4000m"
        "vpa.k8s.io/max-allowed-memory"       = var.environment == "prod" ? "16Gi" : "8Gi"
        "vpa.k8s.io/cpu-histogram-bucket-size" = "0.1"
        "vpa.k8s.io/memory-histogram-bucket-size" = "100Mi"
        "vpa.k8s.io/recommendation-margin"     = "15%"
      }
    }
    spec = {
      targetRef = {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        name       = "${var.project_name}-api"
      }
      
      updatePolicy = {
        updateMode = var.environment == "prod" ? "Auto" : "Off"
        
        # Controlled resource updates in production
        evictionRequirements = var.environment == "prod" ? [
          {
            resources = ["cpu", "memory"]
            changeRequirement = "TargetHigherThanRequests"
          }
        ] : []
        
        # Minimum time between updates
        minReplicas = var.environment == "prod" ? 1 : 0
      }
      
      resourcePolicy = {
        containerPolicies = [
          {
            containerName = "api"
            mode         = "Auto"
            
            minAllowed = {
              cpu    = var.environment == "prod" ? "100m" : "50m"
              memory = var.environment == "prod" ? "256Mi" : "128Mi"
            }
            
            maxAllowed = {
              cpu    = var.environment == "prod" ? "8000m" : "4000m"
              memory = var.environment == "prod" ? "16Gi" : "8Gi"
            }
            
            # Controlled resource change policy
            controlledResources = ["cpu", "memory"]
            
            # Resource scaling factors
            controlledValues = "RequestsAndLimits"
          }
        ]
      }
      
      # Advanced recommendation policy
      recommenders = [
        {
          name = "default"
        },
        {
          name = "performance-optimized"
        }
      ]
    }
  }
}

# ============================================================================
# VPA FOR ETL SERVICE WITH WORKLOAD-AWARE OPTIMIZATION
# ============================================================================

resource "kubernetes_manifest" "vpa_etl" {
  manifest = {
    apiVersion = "autoscaling.k8s.io/v1"
    kind       = "VerticalPodAutoscaler"
    metadata = {
      name      = "${var.project_name}-etl-vpa"
      namespace = var.namespace
      labels = merge(var.labels, {
        component = "vpa"
        service   = "etl"
        tier      = "workload-aware"
      })
      annotations = {
        "vpa.k8s.io/update-mode"              = var.environment == "prod" ? "Auto" : "Off"
        "vpa.k8s.io/resource-policy"         = "batch-optimized"
        "vpa.k8s.io/min-allowed-cpu"        = "500m"
        "vpa.k8s.io/min-allowed-memory"     = "1Gi"
        "vpa.k8s.io/max-allowed-cpu"        = var.environment == "prod" ? "32000m" : "16000m"
        "vpa.k8s.io/max-allowed-memory"     = var.environment == "prod" ? "64Gi" : "32Gi"
        "vpa.k8s.io/workload-type"          = "batch"
        "vpa.k8s.io/scaling-factor"         = "aggressive"
      }
    }
    spec = {
      targetRef = {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        name       = "${var.project_name}-etl"
      }
      
      updatePolicy = {
        updateMode = var.environment == "prod" ? "Auto" : "Off"
        
        # Batch workload specific update policy
        evictionRequirements = [
          {
            resources = ["cpu", "memory"]
            changeRequirement = "TargetHigherThanRequests"
          }
        ]
        
        # Allow aggressive resource changes for batch workloads
        minReplicas = 0  # Allow scale to zero
      }
      
      resourcePolicy = {
        containerPolicies = [
          {
            containerName = "etl"
            mode         = "Auto"
            
            minAllowed = {
              cpu    = "500m"  # Minimum for ETL processing
              memory = "1Gi"   # Minimum for data operations
            }
            
            maxAllowed = {
              cpu    = var.environment == "prod" ? "32000m" : "16000m"  # High CPU for data processing
              memory = var.environment == "prod" ? "64Gi" : "32Gi"      # High memory for large datasets
            }
            
            # ETL-specific resource controls
            controlledResources = ["cpu", "memory"]
            controlledValues   = "RequestsAndLimits"
            
            # Custom resource recommendations based on data volume
            annotations = {
              "vpa.k8s.io/cpu-multiplier"    = "2.0"  # Aggressive CPU scaling
              "vpa.k8s.io/memory-multiplier" = "1.5"  # Conservative memory scaling
            }
          }
        ]
      }
      
      recommenders = [
        {
          name = "batch-workload-recommender"
        }
      ]
    }
  }
}

# ============================================================================
# VPA FOR DAGSTER WITH EXECUTION-OPTIMIZED RESOURCES
# ============================================================================

resource "kubernetes_manifest" "vpa_dagster" {
  manifest = {
    apiVersion = "autoscaling.k8s.io/v1"
    kind       = "VerticalPodAutoscaler"
    metadata = {
      name      = "${var.project_name}-dagster-vpa"
      namespace = var.namespace
      labels = merge(var.labels, {
        component = "vpa"
        service   = "dagster"
        tier      = "execution-optimized"
      })
      annotations = {
        "vpa.k8s.io/update-mode"            = var.environment == "prod" ? "Auto" : "Off"
        "vpa.k8s.io/resource-policy"       = "orchestrator-optimized"
        "vpa.k8s.io/min-allowed-cpu"      = "250m"
        "vpa.k8s.io/min-allowed-memory"   = "512Mi"
        "vpa.k8s.io/max-allowed-cpu"      = var.environment == "prod" ? "16000m" : "8000m"
        "vpa.k8s.io/max-allowed-memory"   = var.environment == "prod" ? "32Gi" : "16Gi"
        "vpa.k8s.io/execution-aware"      = "true"
        "vpa.k8s.io/pipeline-coordination" = "enabled"
      }
    }
    spec = {
      targetRef = {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        name       = "${var.project_name}-dagster"
      }
      
      updatePolicy = {
        updateMode = var.environment == "prod" ? "Auto" : "Off"
        
        # Orchestrator-specific update policy
        evictionRequirements = [
          {
            resources = ["cpu", "memory"]
            changeRequirement = "TargetHigherThanRequests"
          }
        ]
        
        # Maintain minimum replicas for coordination
        minReplicas = 1
      }
      
      resourcePolicy = {
        containerPolicies = [
          {
            containerName = "dagster"
            mode         = "Auto"
            
            minAllowed = {
              cpu    = "250m"  # Minimum for orchestration
              memory = "512Mi" # Minimum for pipeline state
            }
            
            maxAllowed = {
              cpu    = var.environment == "prod" ? "16000m" : "8000m"  # High CPU for complex pipelines
              memory = var.environment == "prod" ? "32Gi" : "16Gi"     # High memory for pipeline coordination
            }
            
            # Orchestrator-specific resource controls
            controlledResources = ["cpu", "memory"]
            controlledValues   = "RequestsAndLimits"
            
            # Pipeline execution aware scaling
            annotations = {
              "vpa.k8s.io/execution-scaling"   = "enabled"
              "vpa.k8s.io/coordination-factor" = "1.2"  # 20% overhead for coordination
            }
          }
        ]
      }
      
      recommenders = [
        {
          name = "orchestrator-recommender"
        }
      ]
    }
  }
}

# ============================================================================
# VPA FOR ELASTICSEARCH WITH MEMORY-OPTIMIZED CONFIGURATION
# ============================================================================

resource "kubernetes_manifest" "vpa_elasticsearch" {
  manifest = {
    apiVersion = "autoscaling.k8s.io/v1"
    kind       = "VerticalPodAutoscaler"
    metadata = {
      name      = "${var.project_name}-elasticsearch-vpa"
      namespace = var.namespace
      labels = merge(var.labels, {
        component = "vpa"
        service   = "elasticsearch"
        tier      = "memory-optimized"
      })
      annotations = {
        "vpa.k8s.io/update-mode"             = var.environment == "prod" ? "Off" : "Off"  # Manual for ES
        "vpa.k8s.io/resource-policy"        = "memory-optimized"
        "vpa.k8s.io/min-allowed-cpu"       = "1000m"
        "vpa.k8s.io/min-allowed-memory"    = "2Gi"
        "vpa.k8s.io/max-allowed-cpu"       = var.environment == "prod" ? "8000m" : "4000m"
        "vpa.k8s.io/max-allowed-memory"    = var.environment == "prod" ? "32Gi" : "16Gi"
        "vpa.k8s.io/jvm-heap-optimization" = "enabled"
        "vpa.k8s.io/gc-optimization"       = "g1gc"
      }
    }
    spec = {
      targetRef = {
        apiVersion = "apps/v1"
        kind       = "Deployment"
        name       = "${var.project_name}-elasticsearch"
      }
      
      updatePolicy = {
        updateMode = "Off"  # Manual updates for Elasticsearch
        
        # Elasticsearch requires careful resource management
        evictionRequirements = []
        
        # Always maintain minimum replicas for cluster health
        minReplicas = 1
      }
      
      resourcePolicy = {
        containerPolicies = [
          {
            containerName = "elasticsearch"
            mode         = "Off"  # Provide recommendations only
            
            minAllowed = {
              cpu    = "1000m"  # Minimum for ES performance
              memory = "2Gi"    # Minimum for JVM heap
            }
            
            maxAllowed = {
              cpu    = var.environment == "prod" ? "8000m" : "4000m"
              memory = var.environment == "prod" ? "32Gi" : "16Gi"
            }
            
            # Elasticsearch-specific resource controls
            controlledResources = ["cpu", "memory"]
            controlledValues   = "RequestsAndLimits"
            
            # JVM heap optimization
            annotations = {
              "vpa.k8s.io/heap-ratio"         = "0.5"   # 50% of memory for heap
              "vpa.k8s.io/direct-memory-ratio" = "0.3"   # 30% for direct memory
              "vpa.k8s.io/system-reserved"    = "0.2"   # 20% for system
            }
          }
        ]
      }
      
      recommenders = [
        {
          name = "elasticsearch-recommender"
        }
      ]
    }
  }
}

# ============================================================================
# VPA RECOMMENDER CONFIGURATION
# ============================================================================

# Custom VPA recommender for workload-specific optimization
resource "kubernetes_deployment" "vpa_recommender_enhanced" {
  metadata {
    name      = "${var.project_name}-vpa-recommender"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "vpa-recommender"
      tier      = "enhanced"
    })
  }
  
  spec {
    replicas = var.environment == "prod" ? 2 : 1
    
    selector {
      match_labels = {
        app       = "${var.project_name}-vpa-recommender"
        component = "vpa-recommender"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-vpa-recommender"
          component = "vpa-recommender"
        })
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }
      
      spec {
        service_account_name = var.service_account_name
        
        container {
          name  = "recommender"
          image = "k8s.gcr.io/autoscaling/vpa-recommender:0.14.0"
          
          port {
            name           = "metrics"
            container_port = 8080
            protocol       = "TCP"
          }
          
          command = [
            "./recommender",
            "--v=4",
            "--stderrthreshold=info",
            "--pod-recommendation-min-cpu-millicores=25",
            "--pod-recommendation-min-memory-mb=100",
            "--target-cpu-percentile=0.9",
            "--recommendation-margin-fraction=0.15",
            "--oom-min-bump-up-bytes=104857600",  # 100MB
            "--oom-bump-up-ratio=1.2",
            "--memory-aggregation-interval=1h",
            "--memory-aggregation-interval-count=8",
            "--memory-histogram-decay-half-life=24h0m0s",
            "--cpu-aggregation-interval=1h",
            "--cpu-aggregation-interval-count=8",
            "--cpu-histogram-decay-half-life=24h0m0s",
            "--checkpoints-timeout=1m0s",
            "--min-checkpoints=10",
            "--memory-saver=false",
            "--recommender-interval=1m0s",
            "--checkpoints-gc-interval=10m0s",
            "--prometheus-address=http://prometheus.monitoring:9090",
            "--prometheus-cadvisor-job-name=kubernetes-cadvisor",
            "--address=0.0.0.0:8080",
            "--kubeconfig=/dev/null"
          ]
          
          env {
            name  = "NAMESPACE"
            value = var.namespace
          }
          
          env {
            name = "OOM_KILL_DISABLED"
            value_from {
              field_ref {
                field_path = "spec.nodeName"
              }
            }
          }
          
          resources {
            requests = {
              cpu    = "50m"
              memory = "500Mi"
            }
            limits = {
              cpu    = var.environment == "prod" ? "1000m" : "500m"
              memory = var.environment == "prod" ? "2Gi" : "1Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health-check"
              port = 8080
              scheme = "HTTP"
            }
            initial_delay_seconds = 15
            period_seconds        = 10
            timeout_seconds       = 5
            failure_threshold     = 6
            success_threshold     = 1
          }
          
          readiness_probe {
            http_get {
              path = "/health-check"
              port = 8080
              scheme = "HTTP"
            }
            initial_delay_seconds = 5
            period_seconds        = 3
            timeout_seconds       = 1
            failure_threshold     = 120
            success_threshold     = 1
          }
          
          volume_mount {
            name       = "vpa-tls-certs"
            mount_path = "/etc/tls-certs"
            read_only  = true
          }
        }
        
        volume {
          name = "vpa-tls-certs"
          secret {
            secret_name = "${var.project_name}-vpa-tls"
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# ============================================================================
# VPA UPDATER CONFIGURATION
# ============================================================================

# VPA Updater for automatic resource updates
resource "kubernetes_deployment" "vpa_updater_enhanced" {
  metadata {
    name      = "${var.project_name}-vpa-updater"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "vpa-updater"
      tier      = "enhanced"
    })
  }
  
  spec {
    replicas = 1  # Only one updater needed
    
    selector {
      match_labels = {
        app       = "${var.project_name}-vpa-updater"
        component = "vpa-updater"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-vpa-updater"
          component = "vpa-updater"
        })
      }
      
      spec {
        service_account_name = var.service_account_name
        
        container {
          name  = "updater"
          image = "k8s.gcr.io/autoscaling/vpa-updater:0.14.0"
          
          command = [
            "./updater",
            "--v=4",
            "--stderrthreshold=info",
            "--min-replicas=1",
            "--eviction-tolerance=0.5",
            "--eviction-rate-limit=10",
            "--eviction-rate-burst=1",
            "--updater-interval=1m0s",
            "--use-admission-controller-status=true"
          ]
          
          env {
            name  = "NAMESPACE"
            value = var.namespace
          }
          
          resources {
            requests = {
              cpu    = "30m"
              memory = "200Mi"
            }
            limits = {
              cpu    = "200m"
              memory = "1Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health-check"
              port = 8080
            }
            initial_delay_seconds = 15
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/health-check"
              port = 8080
            }
            initial_delay_seconds = 5
            period_seconds        = 3
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# ============================================================================
# VPA ADMISSION CONTROLLER
# ============================================================================

# VPA Admission Controller for resource injection
resource "kubernetes_deployment" "vpa_admission_controller" {
  metadata {
    name      = "${var.project_name}-vpa-admission-controller"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "vpa-admission-controller"
      tier      = "enhanced"
    })
  }
  
  spec {
    replicas = var.environment == "prod" ? 2 : 1
    
    selector {
      match_labels = {
        app       = "${var.project_name}-vpa-admission-controller"
        component = "vpa-admission-controller"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-vpa-admission-controller"
          component = "vpa-admission-controller"
        })
      }
      
      spec {
        service_account_name = var.service_account_name
        
        container {
          name  = "admission-controller"
          image = "k8s.gcr.io/autoscaling/vpa-admission-controller:0.14.0"
          
          port {
            container_port = 8000
            protocol       = "TCP"
          }
          
          port {
            container_port = 8080
            protocol       = "TCP"
          }
          
          command = [
            "./admission-controller",
            "--v=4",
            "--stderrthreshold=info",
            "--address=0.0.0.0:8000",
            "--port=8000",
            "--certs-dir=/etc/tls-certs",
            "--client-ca-file=/etc/tls-certs/caCert.pem",
            "--tls-cert-file=/etc/tls-certs/serverCert.pem",
            "--tls-private-key=/etc/tls-certs/serverKey.pem",
            "--webhook-timeout-seconds=30",
            "--webhook-port=8000",
            "--register-webhook=true",
            "--register-by-url=false"
          ]
          
          env {
            name  = "NAMESPACE"
            value = var.namespace
          }
          
          resources {
            requests = {
              cpu    = "50m"
              memory = "200Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "1Gi"
            }
          }
          
          liveness_probe {
            http_get {
              path = "/health-check"
              port = 8080
              scheme = "HTTP"
            }
            initial_delay_seconds = 15
            period_seconds        = 10
          }
          
          readiness_probe {
            http_get {
              path = "/health-check"
              port = 8080
              scheme = "HTTP"
            }
            initial_delay_seconds = 5
            period_seconds        = 3
          }
          
          volume_mount {
            name       = "vpa-tls-certs"
            mount_path = "/etc/tls-certs"
            read_only  = true
          }
        }
        
        volume {
          name = "vpa-tls-certs"
          secret {
            secret_name = "${var.project_name}-vpa-tls"
          }
        }
        
        restart_policy = "Always"
      }
    }
  }
}

# Service for VPA admission controller
resource "kubernetes_service" "vpa_admission_controller" {
  metadata {
    name      = "vpa-webhook"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "vpa-admission-controller"
    })
  }
  
  spec {
    selector = {
      app       = "${var.project_name}-vpa-admission-controller"
      component = "vpa-admission-controller"
    }
    
    port {
      name        = "webhook"
      port        = 443
      target_port = 8000
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# ============================================================================
# RESOURCE OPTIMIZATION POLICIES
# ============================================================================

# ConfigMap for advanced VPA policies
resource "kubernetes_config_map" "vpa_optimization_policies" {
  metadata {
    name      = "${var.project_name}-vpa-policies"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "vpa-policies"
    })
  }
  
  data = {
    "resource-optimization-policy" = jsonencode({
      workload_profiles = {
        api_service = {
          cpu_percentile = 90
          memory_percentile = 95
          scaling_factor = 1.2
          safety_margin = 0.15
          update_frequency = "1h"
        }
        
        etl_batch = {
          cpu_percentile = 95
          memory_percentile = 90
          scaling_factor = 1.5
          safety_margin = 0.20
          update_frequency = "30m"
        }
        
        orchestrator = {
          cpu_percentile = 85
          memory_percentile = 90
          scaling_factor = 1.3
          safety_margin = 0.25
          update_frequency = "15m"
        }
        
        search_engine = {
          cpu_percentile = 80
          memory_percentile = 95
          scaling_factor = 1.1
          safety_margin = 0.10
          update_frequency = "2h"
        }
      }
      
      cost_optimization = {
        enable_aggressive_downscaling = var.enable_cost_optimization
        minimum_resource_guarantee = 0.1
        maximum_waste_tolerance = 0.15
        prefer_requests_over_limits = true
      }
      
      performance_optimization = {
        optimize_for_latency = var.environment == "prod"
        optimize_for_throughput = true
        enable_burst_scaling = true
        performance_sla_cpu_threshold = 85
        performance_sla_memory_threshold = 80
      }
    })
    
    "recommendation-filters" = jsonencode({
      cpu_filters = {
        min_change_threshold = "50m"
        max_change_ratio = 2.0
        ignore_short_spikes = true
        spike_duration_threshold = "5m"
      }
      
      memory_filters = {
        min_change_threshold = "128Mi"
        max_change_ratio = 2.0
        oom_protection_factor = 1.3
        gc_overhead_factor = 1.2
      }
      
      update_policies = {
        max_updates_per_hour = 4
        min_time_between_updates = "15m"
        require_stable_recommendations = true
        stability_threshold = "10m"
      }
    })
  }
}