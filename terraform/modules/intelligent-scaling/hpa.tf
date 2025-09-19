# Enhanced Horizontal Pod Autoscaler with Custom Metrics and Predictive Scaling
# Production-ready HPA configuration with custom metrics from Prometheus

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
  }
}

# ============================================================================
# CUSTOM METRICS SERVER INTEGRATION
# ============================================================================

# Custom Metrics API configuration for business metrics
resource "kubernetes_config_map" "custom_metrics_config" {
  metadata {
    name      = "${var.project_name}-custom-metrics-config"
    namespace = var.namespace
    labels    = var.labels
  }
  
  data = {
    "prometheus-url"    = "http://prometheus.monitoring:9090"
    "metrics-interval"  = "30s"
    "cache-duration"   = "60s"
    "enable-debug"     = var.enable_debug ? "true" : "false"
    
    # Custom business metrics configuration
    "business-metrics" = jsonencode({
      api_requests_per_second = {
        query = "sum(rate(http_requests_total{service=\"api\"}[2m]))"
        threshold = 100
        scale_up_threshold = 80
        scale_down_threshold = 20
      }
      data_processing_queue_length = {
        query = "sum(rabbitmq_queue_messages{queue=~\"data.*\"})"
        threshold = 1000
        scale_up_threshold = 800
        scale_down_threshold = 200
      }
      database_connection_pool_usage = {
        query = "sum(pg_stat_activity_count) / sum(pg_settings_max_connections) * 100"
        threshold = 80
        scale_up_threshold = 70
        scale_down_threshold = 30
      }
    })
  }
}

# ============================================================================
# ENHANCED HPA FOR API SERVICE
# ============================================================================

resource "kubernetes_horizontal_pod_autoscaler_v2" "api_enhanced" {
  metadata {
    name      = "${var.project_name}-api-hpa-enhanced"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "autoscaling"
      service   = "api"
      tier      = "enhanced"
    })
    annotations = {
      "scaling.kubernetes.io/predictive"        = "enabled"
      "scaling.kubernetes.io/learning-period"   = "168h"  # 1 week
      "scaling.kubernetes.io/forecast-horizon"  = "30m"
      "autoscaling.alpha.kubernetes.io/behavior" = "predictive"
    }
  }
  
  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = "${var.project_name}-api"
    }
    
    # Production-ready scaling configuration
    min_replicas = var.environment == "prod" ? 3 : 1
    max_replicas = var.environment == "prod" ? 50 : 10
    
    # CPU utilization metric with optimized thresholds
    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = var.environment == "prod" ? 60 : 70  # Lower threshold for prod
        }
      }
    }
    
    # Memory utilization metric
    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = var.environment == "prod" ? 70 : 80
        }
      }
    }
    
    # Custom metric: API requests per second
    metric {
      type = "External"
      external {
        metric {
          name = "api_requests_per_second"
          selector {
            match_labels = {
              service = "api"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = var.environment == "prod" ? "100" : "50"
        }
      }
    }
    
    # Custom metric: Response time P95
    metric {
      type = "External"
      external {
        metric {
          name = "api_response_time_p95"
          selector {
            match_labels = {
              service = "api"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = var.environment == "prod" ? "200" : "500"  # milliseconds
        }
      }
    }
    
    # Custom metric: Database connection pool usage
    metric {
      type = "External"
      external {
        metric {
          name = "database_connection_pool_usage"
          selector {
            match_labels = {
              service = "api"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = "70"  # 70% pool usage triggers scaling
        }
      }
    }
    
    # Advanced scaling behavior with predictive capabilities
    behavior {
      scale_up {
        stabilization_window_seconds = var.environment == "prod" ? 60 : 120
        select_policy               = "Max"
        
        # Aggressive scaling for traffic spikes
        policy {
          type          = "Percent"
          value         = var.environment == "prod" ? 200 : 100  # 200% increase for prod
          period_seconds = 15
        }
        
        # Conservative scaling for sustained load
        policy {
          type          = "Pods"
          value         = var.environment == "prod" ? 5 : 2
          period_seconds = 60
        }
      }
      
      scale_down {
        stabilization_window_seconds = var.environment == "prod" ? 300 : 180
        select_policy               = "Min"
        
        # Gradual scale down to prevent thrashing
        policy {
          type          = "Percent"
          value         = 25  # Maximum 25% reduction
          period_seconds = 60
        }
        
        # Conservative pod removal
        policy {
          type          = "Pods"
          value         = var.environment == "prod" ? 2 : 1
          period_seconds = 120
        }
      }
    }
  }
}

# ============================================================================
# ENHANCED HPA FOR ETL SERVICE WITH QUEUE-BASED SCALING
# ============================================================================

resource "kubernetes_horizontal_pod_autoscaler_v2" "etl_queue_based" {
  metadata {
    name      = "${var.project_name}-etl-hpa-queue"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "autoscaling"
      service   = "etl"
      tier      = "queue-based"
    })
    annotations = {
      "scaling.kubernetes.io/queue-based" = "enabled"
      "scaling.kubernetes.io/queue-type"  = "rabbitmq"
    }
  }
  
  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = "${var.project_name}-etl"
    }
    
    # Queue-based scaling configuration
    min_replicas = 0  # Scale to zero when no work
    max_replicas = var.environment == "prod" ? 20 : 10
    
    # CPU utilization for resource optimization
    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = 80
        }
      }
    }
    
    # Memory utilization
    metric {
      type = "Resource"
      resource {
        name = "memory"
        target {
          type                = "Utilization"
          average_utilization = 85
        }
      }
    }
    
    # Queue length metric for data processing
    metric {
      type = "External"
      external {
        metric {
          name = "rabbitmq_queue_messages"
          selector {
            match_labels = {
              queue = "data-processing"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = "10"  # 10 messages per pod
        }
      }
    }
    
    # High priority queue metric
    metric {
      type = "External"
      external {
        metric {
          name = "rabbitmq_queue_messages_high_priority"
          selector {
            match_labels = {
              queue = "data-processing-high"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = "5"  # 5 high priority messages per pod
        }
      }
    }
    
    # Queue-optimized scaling behavior
    behavior {
      scale_up {
        stabilization_window_seconds = 30  # Quick response to queue buildup
        select_policy               = "Max"
        
        # Rapid scaling for queue backlog
        policy {
          type          = "Percent"
          value         = 300  # 300% increase for queue bursts
          period_seconds = 15
        }
        
        # Sustained scaling for consistent load
        policy {
          type          = "Pods"
          value         = var.environment == "prod" ? 10 : 5
          period_seconds = 30
        }
      }
      
      scale_down {
        stabilization_window_seconds = 600  # 10 minutes to ensure queue is processed
        select_policy               = "Min"
        
        # Conservative scale down for queue processing
        policy {
          type          = "Percent"
          value         = 50  # Maximum 50% reduction
          period_seconds = 120
        }
        
        # Gradual pod removal
        policy {
          type          = "Pods"
          value         = var.environment == "prod" ? 3 : 2
          period_seconds = 180
        }
      }
    }
  }
}

# ============================================================================
# ENHANCED HPA FOR DAGSTER WITH EXECUTION-BASED SCALING
# ============================================================================

resource "kubernetes_horizontal_pod_autoscaler_v2" "dagster_execution_based" {
  metadata {
    name      = "${var.project_name}-dagster-hpa-execution"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "autoscaling"
      service   = "dagster"
      tier      = "execution-based"
    })
    annotations = {
      "scaling.kubernetes.io/execution-based" = "enabled"
      "scaling.kubernetes.io/pipeline-aware"  = "true"
    }
  }
  
  spec {
    scale_target_ref {
      api_version = "apps/v1"
      kind        = "Deployment"
      name        = "${var.project_name}-dagster"
    }
    
    # Pipeline execution-based scaling
    min_replicas = 1  # Always keep one instance for pipeline coordination
    max_replicas = var.environment == "prod" ? 15 : 8
    
    # CPU utilization for pipeline execution
    metric {
      type = "Resource"
      resource {
        name = "cpu"
        target {
          type                = "Utilization"
          average_utilization = 75
        }
      }
    }
    
    # Memory utilization for pipeline state management
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
    
    # Active pipeline executions metric
    metric {
      type = "External"
      external {
        metric {
          name = "dagster_active_runs"
          selector {
            match_labels = {
              service = "dagster"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = "2"  # 2 active runs per instance
        }
      }
    }
    
    # Queued pipeline runs metric
    metric {
      type = "External"
      external {
        metric {
          name = "dagster_queued_runs"
          selector {
            match_labels = {
              service = "dagster"
              environment = var.environment
            }
          }
        }
        target {
          type          = "AverageValue"
          average_value = "3"  # 3 queued runs per instance
        }
      }
    }
    
    # Pipeline execution optimized scaling behavior
    behavior {
      scale_up {
        stabilization_window_seconds = 45  # Quick response to pipeline load
        select_policy               = "Max"
        
        # Moderate scaling for pipeline coordination
        policy {
          type          = "Percent"
          value         = 100  # 100% increase for pipeline bursts
          period_seconds = 30
        }
        
        # Controlled scaling for execution coordination
        policy {
          type          = "Pods"
          value         = var.environment == "prod" ? 3 : 2
          period_seconds = 45
        }
      }
      
      scale_down {
        stabilization_window_seconds = 600  # 10 minutes for pipeline completion
        select_policy               = "Min"
        
        # Conservative scale down to maintain coordination
        policy {
          type          = "Percent"
          value         = 30  # Maximum 30% reduction
          period_seconds = 120
        }
        
        # Careful pod removal to maintain pipeline state
        policy {
          type          = "Pods"
          value         = 1  # Remove one pod at a time
          period_seconds = 300
        }
      }
    }
  }
}

# ============================================================================
# MULTI-DIMENSIONAL SCALING COORDINATION
# ============================================================================

# Custom resource for coordinating multiple HPAs
resource "kubernetes_config_map" "scaling_coordination" {
  metadata {
    name      = "${var.project_name}-scaling-coordination"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "autoscaling-coordination"
    })
  }
  
  data = {
    "coordination-policy" = jsonencode({
      global_scaling_limits = {
        max_total_pods = var.environment == "prod" ? 100 : 50
        max_resource_allocation = {
          cpu = var.environment == "prod" ? "200" : "100"  # cores
          memory = var.environment == "prod" ? "400Gi" : "200Gi"
        }
      }
      
      service_dependencies = {
        api = ["database", "rabbitmq", "elasticsearch"]
        etl = ["database", "rabbitmq", "api"]
        dagster = ["database", "rabbitmq", "api", "etl"]
      }
      
      scaling_priorities = {
        high = ["api"]
        medium = ["etl"]
        low = ["dagster"]
      }
      
      cost_optimization = {
        enable_scale_to_zero = true
        scale_to_zero_grace_period = "300s"
        prefer_spot_instances = var.enable_spot_instances
        cost_threshold_per_hour = var.cost_threshold_per_hour
      }
    })
  }
}

# ============================================================================
# PREDICTIVE SCALING CONFIGURATION
# ============================================================================

# ConfigMap for predictive scaling machine learning models
resource "kubernetes_config_map" "predictive_scaling_models" {
  metadata {
    name      = "${var.project_name}-predictive-models"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "predictive-scaling"
    })
  }
  
  data = {
    "model-config" = jsonencode({
      api_traffic_model = {
        type = "time_series"
        algorithm = "arima"
        training_window = "7d"
        forecast_horizon = "1h"
        confidence_interval = 0.95
        features = [
          "hour_of_day",
          "day_of_week",
          "is_weekend",
          "is_holiday",
          "previous_requests",
          "trend_component"
        ]
      }
      
      queue_depth_model = {
        type = "regression"
        algorithm = "xgboost"
        training_window = "3d"
        forecast_horizon = "30m"
        features = [
          "current_queue_depth",
          "processing_rate",
          "arrival_rate",
          "time_since_last_batch",
          "available_workers"
        ]
      }
      
      resource_usage_model = {
        type = "multivariate_time_series"
        algorithm = "lstm"
        training_window = "14d"
        forecast_horizon = "2h"
        features = [
          "cpu_usage",
          "memory_usage",
          "network_io",
          "disk_io",
          "active_connections",
          "request_volume"
        ]
      }
    })
    
    "scaling-thresholds" = jsonencode({
      confidence_thresholds = {
        scale_up = 0.8
        scale_down = 0.9
      }
      
      forecast_accuracy_requirements = {
        minimum_accuracy = 0.75
        retrain_threshold = 0.7
      }
      
      prediction_weights = {
        traffic_model = 0.4
        queue_model = 0.35
        resource_model = 0.25
      }
    })
  }
}

# ============================================================================
# SCALING EVENTS AND MONITORING
# ============================================================================

# Service monitor for HPA metrics collection
resource "kubernetes_manifest" "hpa_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "${var.project_name}-hpa-metrics"
      namespace = var.namespace
      labels = merge(var.labels, {
        component = "monitoring"
        tier      = "autoscaling"
      })
    }
    spec = {
      selector = {
        matchLabels = {
          component = "autoscaling"
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

# Custom metrics exporter deployment
resource "kubernetes_deployment" "custom_metrics_exporter" {
  metadata {
    name      = "${var.project_name}-metrics-exporter"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "metrics-exporter"
      tier      = "monitoring"
    })
  }
  
  spec {
    replicas = 1
    
    selector {
      match_labels = {
        app       = "${var.project_name}-metrics-exporter"
        component = "metrics-exporter"
      }
    }
    
    template {
      metadata {
        labels = merge(var.labels, {
          app       = "${var.project_name}-metrics-exporter"
          component = "metrics-exporter"
        })
      }
      
      spec {
        service_account_name = var.service_account_name
        
        container {
          name  = "metrics-exporter"
          image = "prom/custom-metrics-exporter:latest"
          
          port {
            name           = "metrics"
            container_port = 8080
            protocol       = "TCP"
          }
          
          env {
            name = "PROMETHEUS_URL"
            value_from {
              config_map_key_ref {
                name = kubernetes_config_map.custom_metrics_config.metadata[0].name
                key  = "prometheus-url"
              }
            }
          }
          
          env {
            name = "METRICS_CONFIG"
            value_from {
              config_map_key_ref {
                name = kubernetes_config_map.custom_metrics_config.metadata[0].name
                key  = "business-metrics"
              }
            }
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
        
        restart_policy = "Always"
      }
    }
  }
}

# Service for custom metrics exporter
resource "kubernetes_service" "custom_metrics_exporter" {
  metadata {
    name      = "${var.project_name}-metrics-exporter-service"
    namespace = var.namespace
    labels = merge(var.labels, {
      component = "metrics-exporter"
    })
    annotations = {
      "prometheus.io/scrape" = "true"
      "prometheus.io/port"   = "8080"
      "prometheus.io/path"   = "/metrics"
    }
  }
  
  spec {
    selector = {
      app       = "${var.project_name}-metrics-exporter"
      component = "metrics-exporter"
    }
    
    port {
      name        = "metrics"
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}