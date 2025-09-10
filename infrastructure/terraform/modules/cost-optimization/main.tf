# BMAD Platform - Cost Optimization Module for 40% Infrastructure Reduction
# Intelligent resource management with automated cost optimization strategies

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
  
  # Cost optimization targets
  target_reduction = var.cost_reduction_target  # 40% target reduction
  
  # Budget allocations
  budget_config = {
    aws_monthly   = var.aws_cost_budget
    azure_monthly = var.azure_cost_budget
    gcp_monthly   = var.gcp_cost_budget
    total_monthly = var.aws_cost_budget + var.azure_cost_budget + var.gcp_cost_budget
  }
  
  # Optimization strategies
  optimization_strategies = {
    spot_instances      = var.enable_spot_instances
    reserved_instances  = var.enable_reserved_instances
    scheduled_scaling   = var.enable_scheduled_scaling
    resource_tagging    = var.enable_resource_tagging
    auto_shutdown      = var.enable_auto_shutdown
  }
  
  # Scheduling configuration
  schedule_config = {
    shutdown_schedule = var.non_prod_shutdown_schedule
    startup_schedule  = var.non_prod_startup_schedule
    timezone         = "UTC"
  }
}

# Cost Optimization Namespace
resource "kubernetes_namespace" "cost_optimization" {
  metadata {
    name = "cost-optimization"
    
    labels = merge(local.common_tags, {
      purpose = "cost-optimization"
      target-reduction = "${var.cost_reduction_target * 100}%"
    })
  }
}

# AWS Cost Management
# ==================

# AWS Budgets for Cost Control
resource "aws_budgets_budget" "monthly_budget" {
  count = var.aws_cost_budget > 0 ? 1 : 0
  
  name         = "${var.project_name}-monthly-budget"
  budget_type  = "COST"
  limit_amount = tostring(local.budget_config.aws_monthly)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"
  
  cost_filters {
    tag {
      key = "Project"
      values = [var.project_name]
    }
  }
  
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
  
  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
  
  tags = local.common_tags
}

# AWS Cost Anomaly Detection
resource "aws_ce_anomaly_detector" "cost_anomaly" {
  count = var.aws_cost_budget > 0 ? 1 : 0
  
  name         = "${var.project_name}-cost-anomaly-detector"
  monitor_type = "DIMENSIONAL"
  
  specification = jsonencode({
    Dimension = "SERVICE"
    MatchOptions = ["EQUALS"]
    Values = ["Amazon Elastic Compute Cloud - Compute", "Amazon Relational Database Service"]
  })
  
  tags = local.common_tags
}

# AWS Reserved Instance Recommendations
resource "aws_ce_reservation_recommendation" "ri_recommendations" {
  count = local.optimization_strategies.reserved_instances ? 1 : 0
  
  payment_option = "ALL_UPFRONT"
  service        = "Amazon Elastic Compute Cloud - Compute"
  
  tags = local.common_tags
}

# Azure Cost Management
# ====================

# Azure Budget
resource "azurerm_consumption_budget_subscription" "monthly_budget" {
  count = var.azure_cost_budget > 0 ? 1 : 0
  
  name            = "${var.project_name}-monthly-budget"
  subscription_id = data.azurerm_client_config.current.subscription_id
  
  amount     = local.budget_config.azure_monthly
  time_grain = "Monthly"
  
  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
    end_date   = formatdate("YYYY-MM-01'T'00:00:00Z", timeadd(timestamp(), "8760h")) # 1 year
  }
  
  filter {
    tag {
      name = "Project"
      values = [var.project_name]
    }
  }
  
  notification {
    enabled   = true
    threshold = 80
    operator  = "GreaterThan"
    
    contact_emails = [var.alert_email]
  }
  
  notification {
    enabled   = true
    threshold = 100
    operator  = "GreaterThan"
    threshold_type = "Forecasted"
    
    contact_emails = [var.alert_email]
  }
}

# Google Cloud Cost Management
# ============================

# GCP Budget
resource "google_billing_budget" "monthly_budget" {
  count = var.gcp_cost_budget > 0 ? 1 : 0
  
  billing_account = var.gcp_billing_account_id
  display_name    = "${var.project_name}-monthly-budget"
  
  budget_filter {
    projects = ["projects/${var.gcp_project_id}"]
    
    labels = {
      project = var.project_name
    }
  }
  
  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(local.budget_config.gcp_monthly)
    }
  }
  
  threshold_rules {
    threshold_percent = 0.8
    spend_basis      = "CURRENT_SPEND"
  }
  
  threshold_rules {
    threshold_percent = 1.0
    spend_basis      = "FORECASTED_SPEND"
  }
  
  all_updates_rule {
    monitoring_notification_channels = [
      google_monitoring_notification_channel.email[0].id
    ]
  }
}

# GCP Notification Channel
resource "google_monitoring_notification_channel" "email" {
  count = var.gcp_cost_budget > 0 ? 1 : 0
  
  display_name = "Cost Alert Email"
  type         = "email"
  
  labels = {
    email_address = var.alert_email
  }
}

# Kubernetes Cost Optimization
# ============================

# Vertical Pod Autoscaler Admission Controller
resource "helm_release" "vpa" {
  name       = "vpa"
  repository = "https://charts.fairwinds.com/stable"
  chart      = "vpa"
  version    = "4.4.6"
  namespace  = kubernetes_namespace.cost_optimization.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/vpa-values.yaml", {
      admission_controller_enabled = true
      recommender_enabled         = true
      updater_enabled            = true
      metrics_enabled            = true
    })
  ]
}

# KEDA for Event-Driven Autoscaling
resource "helm_release" "keda" {
  name       = "keda"
  repository = "https://kedacore.github.io/charts"
  chart      = "keda"
  version    = "2.12.1"
  namespace  = kubernetes_namespace.cost_optimization.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/keda-values.yaml", {
      metrics_server_enabled = true
      prometheus_enabled     = true
      operator_replicas     = 2
      webhooks_enabled      = true
    })
  ]
}

# Cost Optimization Controller Deployment
resource "kubernetes_deployment" "cost_optimizer" {
  metadata {
    name      = "cost-optimizer"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
    
    labels = {
      app = "cost-optimizer"
    }
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "cost-optimizer"
      }
    }

    template {
      metadata {
        labels = {
          app = "cost-optimizer"
        }
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        container {
          image = "bmad/cost-optimizer:v1.0.0"
          name  = "cost-optimizer"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name  = "TARGET_REDUCTION_PERCENTAGE"
            value = tostring(var.cost_reduction_target * 100)
          }
          
          env {
            name  = "AWS_BUDGET_AMOUNT"
            value = tostring(local.budget_config.aws_monthly)
          }
          
          env {
            name  = "AZURE_BUDGET_AMOUNT"
            value = tostring(local.budget_config.azure_monthly)
          }
          
          env {
            name  = "GCP_BUDGET_AMOUNT"
            value = tostring(local.budget_config.gcp_monthly)
          }
          
          env {
            name  = "OPTIMIZATION_INTERVAL"
            value = "3600"  # 1 hour
          }
          
          env {
            name  = "SPOT_INSTANCES_ENABLED"
            value = tostring(local.optimization_strategies.spot_instances)
          }
          
          env {
            name  = "SCHEDULED_SCALING_ENABLED"
            value = tostring(local.optimization_strategies.scheduled_scaling)
          }
          
          env {
            name  = "AUTO_SHUTDOWN_ENABLED"
            value = tostring(local.optimization_strategies.auto_shutdown)
          }
          
          env {
            name  = "SHUTDOWN_SCHEDULE"
            value = local.schedule_config.shutdown_schedule
          }
          
          env {
            name  = "STARTUP_SCHEDULE"
            value = local.schedule_config.startup_schedule
          }
          
          env {
            name  = "WEBHOOK_URL"
            value = var.cost_webhook_url
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
            name       = "cost-config"
            mount_path = "/etc/cost-optimizer"
            read_only  = true
          }
        }
        
        volume {
          name = "cost-config"
          config_map {
            name = kubernetes_config_map.cost_optimizer_config.metadata[0].name
          }
        }
        
        service_account_name = kubernetes_service_account.cost_optimizer.metadata[0].name
      }
    }
  }
}

# Cost Optimizer Configuration
resource "kubernetes_config_map" "cost_optimizer_config" {
  metadata {
    name      = "cost-optimizer-config"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }

  data = {
    "optimization-rules.yaml" = templatefile("${path.module}/templates/optimization-rules.yaml.tpl", {
      target_reduction_percentage = var.cost_reduction_target * 100
      spot_instance_savings      = 70  # 70% savings with spot instances
      reserved_instance_savings   = 60  # 60% savings with RIs
      right_sizing_savings       = 30  # 30% savings with right-sizing
      scheduled_shutdown_savings  = 50  # 50% savings with scheduled shutdown
    })
    
    "resource-policies.yaml" = templatefile("${path.module}/templates/resource-policies.yaml.tpl", {
      min_cpu_utilization_threshold    = 10  # %
      max_cpu_utilization_threshold    = 80  # %
      min_memory_utilization_threshold = 20  # %
      max_memory_utilization_threshold = 85  # %
      idle_resource_threshold_hours    = 24
      oversized_resource_threshold_days = 7
    })
    
    "shutdown-schedules.yaml" = templatefile("${path.module}/templates/shutdown-schedules.yaml.tpl", {
      dev_shutdown_schedule     = local.schedule_config.shutdown_schedule
      dev_startup_schedule     = local.schedule_config.startup_schedule
      staging_shutdown_schedule = local.schedule_config.shutdown_schedule
      staging_startup_schedule = local.schedule_config.startup_schedule
      timezone                 = local.schedule_config.timezone
    })
  }
}

# Service Account for Cost Optimizer
resource "kubernetes_service_account" "cost_optimizer" {
  metadata {
    name      = "cost-optimizer"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }
}

# ClusterRole for Cost Optimizer
resource "kubernetes_cluster_role" "cost_optimizer" {
  metadata {
    name = "cost-optimizer"
  }

  rule {
    api_groups = [""]
    resources  = ["nodes", "pods", "persistentvolumes", "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "replicasets", "statefulsets", "daemonsets"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }
  
  rule {
    api_groups = ["autoscaling"]
    resources  = ["horizontalpodautoscalers"]
    verbs      = ["get", "list", "watch", "create", "patch", "update", "delete"]
  }
  
  rule {
    api_groups = ["autoscaling.k8s.io"]
    resources  = ["verticalpodautoscalers"]
    verbs      = ["get", "list", "watch", "create", "patch", "update", "delete"]
  }
  
  rule {
    api_groups = ["metrics.k8s.io"]
    resources  = ["nodes", "pods"]
    verbs      = ["get", "list"]
  }
}

# ClusterRoleBinding for Cost Optimizer
resource "kubernetes_cluster_role_binding" "cost_optimizer" {
  metadata {
    name = "cost-optimizer"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.cost_optimizer.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.cost_optimizer.metadata[0].name
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }
}

# Resource Tagging for Cost Tracking
resource "kubernetes_config_map" "resource_tagging_policy" {
  count = local.optimization_strategies.resource_tagging ? 1 : 0
  
  metadata {
    name      = "resource-tagging-policy"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }

  data = {
    "tagging-policy.yaml" = templatefile("${path.module}/templates/tagging-policy.yaml.tpl", {
      required_tags = [
        "project",
        "environment", 
        "team",
        "cost-center",
        "application"
      ]
      default_tags = {
        project     = var.project_name
        environment = var.environment
        managed-by  = "terraform"
      }
    })
  }
}

# Scheduled Scaling CronJob
resource "kubernetes_cron_job" "scheduled_scaling" {
  count = local.optimization_strategies.scheduled_scaling ? 1 : 0
  
  metadata {
    name      = "scheduled-scaling"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }
  
  spec {
    concurrency_policy = "Forbid"
    schedule          = "*/15 * * * *"  # Every 15 minutes
    
    job_template {
      metadata {
        name = "scheduled-scaling"
      }
      
      spec {
        template {
          metadata {
            name = "scheduled-scaling"
          }
          
          spec {
            restart_policy = "OnFailure"
            
            container {
              name  = "scheduler"
              image = "bmad/scheduled-scaler:v1.0.0"
              
              env {
                name  = "SHUTDOWN_SCHEDULE"
                value = local.schedule_config.shutdown_schedule
              }
              
              env {
                name  = "STARTUP_SCHEDULE"
                value = local.schedule_config.startup_schedule
              }
              
              env {
                name  = "TIMEZONE"
                value = local.schedule_config.timezone
              }
              
              env {
                name  = "DRY_RUN"
                value = "false"
              }
              
              resources {
                requests = {
                  cpu    = "100m"
                  memory = "128Mi"
                }
                limits = {
                  cpu    = "200m"
                  memory = "256Mi"
                }
              }
            }
            
            service_account_name = kubernetes_service_account.cost_optimizer.metadata[0].name
          }
        }
      }
    }
  }
}

# Cost Monitoring Service
resource "kubernetes_service" "cost_optimizer" {
  metadata {
    name      = "cost-optimizer"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
    
    labels = {
      app = "cost-optimizer"
    }
  }
  
  spec {
    selector = {
      app = "cost-optimizer"
    }
    
    port {
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# Cost Optimization Metrics ServiceMonitor
resource "kubernetes_manifest" "cost_optimizer_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "cost-optimizer-metrics"
      namespace = kubernetes_namespace.cost_optimization.metadata[0].name
      labels = {
        app = "cost-optimizer"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "cost-optimizer"
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

# Resource Utilization Dashboard
resource "kubernetes_config_map" "cost_dashboard" {
  metadata {
    name      = "cost-optimization-dashboard"
    namespace = kubernetes_namespace.cost_optimization.metadata[0].name
  }

  data = {
    "dashboard.json" = templatefile("${path.module}/templates/cost-dashboard.json.tpl", {
      project_name        = var.project_name
      target_reduction    = var.cost_reduction_target * 100
      aws_budget         = local.budget_config.aws_monthly
      azure_budget       = local.budget_config.azure_monthly
      gcp_budget         = local.budget_config.gcp_monthly
      total_budget       = local.budget_config.total_monthly
    })
  }
}

# Data sources
data "azurerm_client_config" "current" {}

# Output cost optimization summary
output "estimated_monthly_savings" {
  description = "Estimated monthly cost savings"
  value = {
    spot_instances      = local.budget_config.total_monthly * 0.30 # 30% of budget
    reserved_instances  = local.budget_config.total_monthly * 0.15 # 15% of budget
    right_sizing       = local.budget_config.total_monthly * 0.10 # 10% of budget
    scheduled_shutdown = local.budget_config.total_monthly * 0.20 # 20% of budget
    total_savings     = local.budget_config.total_monthly * var.cost_reduction_target
  }
}

output "cost_reduction_percentage" {
  description = "Target cost reduction percentage"
  value       = var.cost_reduction_target * 100
}

output "optimization_recommendations" {
  description = "Cost optimization recommendations"
  value = {
    enable_spot_instances     = local.optimization_strategies.spot_instances
    enable_reserved_instances = local.optimization_strategies.reserved_instances
    enable_scheduled_scaling  = local.optimization_strategies.scheduled_scaling
    enable_auto_shutdown     = local.optimization_strategies.auto_shutdown
    monthly_budget_aws       = local.budget_config.aws_monthly
    monthly_budget_azure     = local.budget_config.azure_monthly
    monthly_budget_gcp       = local.budget_config.gcp_monthly
  }
}