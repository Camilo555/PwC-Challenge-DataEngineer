# Container Orchestration Module Variables

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "cost_center" {
  description = "Cost center for resource tagging"
  type        = string
  default     = "data-engineering"
}

variable "business_unit" {
  description = "Business unit for resource tagging"
  type        = string
  default     = "analytics"
}

# ============================================================================
# MULTI-CLOUD DEPLOYMENT CONFIGURATION
# ============================================================================

variable "deploy_to_aws" {
  description = "Deploy AWS EKS cluster"
  type        = bool
  default     = false
}

variable "deploy_to_azure" {
  description = "Deploy Azure AKS cluster"
  type        = bool
  default     = false
}

variable "deploy_to_gcp" {
  description = "Deploy GCP GKE cluster"
  type        = bool
  default     = false
}

variable "kubernetes_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

# ============================================================================
# AWS EKS CONFIGURATION
# ============================================================================

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_vpc_id" {
  description = "AWS VPC ID"
  type        = string
  default     = ""
}

variable "aws_subnet_ids" {
  description = "AWS subnet IDs for EKS cluster"
  type        = list(string)
  default     = []
}

variable "aws_private_subnet_ids" {
  description = "AWS private subnet IDs for Fargate"
  type        = list(string)
  default     = []
}

variable "eks_endpoint_public_access" {
  description = "Enable public access to EKS API endpoint"
  type        = bool
  default     = true
}

variable "eks_public_access_cidrs" {
  description = "CIDR blocks allowed to access EKS API endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "eks_api_server_access_cidrs" {
  description = "CIDR blocks allowed to access EKS API server"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "eks_node_groups" {
  description = "EKS node groups configuration"
  type = list(object({
    name           = string
    instance_types = list(string)
    capacity_type  = string
    ami_type       = string
    disk_size      = number
    desired_size   = number
    max_size       = number
    min_size       = number
    labels         = map(string)
    taints = list(object({
      key    = string
      value  = string
      effect = string
    }))
  }))
  default = [
    {
      name           = "system"
      instance_types = ["t3.medium"]
      capacity_type  = "ON_DEMAND"
      ami_type       = "AL2_x86_64"
      disk_size      = 20
      desired_size   = 2
      max_size       = 4
      min_size       = 1
      labels = {
        role = "system"
      }
      taints = []
    },
    {
      name           = "workload"
      instance_types = ["t3.large", "t3.xlarge"]
      capacity_type  = "SPOT"
      ami_type       = "AL2_x86_64"
      disk_size      = 50
      desired_size   = 3
      max_size       = 10
      min_size       = 1
      labels = {
        role = "workload"
      }
      taints = []
    }
  ]
}

variable "enable_fargate" {
  description = "Enable AWS Fargate for serverless workloads"
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "CloudWatch logs retention in days"
  type        = number
  default     = 30
}

# ============================================================================
# AZURE AKS CONFIGURATION
# ============================================================================

variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "East US"
}

variable "azure_resource_group_name" {
  description = "Azure resource group name"
  type        = string
  default     = ""
}

variable "azure_subnet_id" {
  description = "Azure subnet ID for AKS cluster"
  type        = string
  default     = ""
}

variable "azure_availability_zones" {
  description = "Azure availability zones"
  type        = list(string)
  default     = ["1", "2", "3"]
}

variable "azure_tenant_id" {
  description = "Azure tenant ID for AAD integration"
  type        = string
  default     = ""
}

variable "aks_system_node_count" {
  description = "AKS system node pool initial node count"
  type        = number
  default     = 2
}

variable "aks_system_vm_size" {
  description = "AKS system node pool VM size"
  type        = string
  default     = "Standard_D2s_v3"
}

variable "aks_system_min_count" {
  description = "AKS system node pool minimum node count"
  type        = number
  default     = 1
}

variable "aks_system_max_count" {
  description = "AKS system node pool maximum node count"
  type        = number
  default     = 5
}

variable "aks_max_pods_per_node" {
  description = "Maximum pods per node"
  type        = number
  default     = 110
}

variable "aks_os_disk_size" {
  description = "OS disk size in GB"
  type        = number
  default     = 30
}

variable "aks_network_plugin" {
  description = "AKS network plugin (azure or kubenet)"
  type        = string
  default     = "azure"
}

variable "aks_network_policy" {
  description = "AKS network policy (azure, calico, or cilium)"
  type        = string
  default     = "azure"
}

variable "aks_service_cidr" {
  description = "AKS service CIDR"
  type        = string
  default     = "10.0.0.0/16"
}

variable "aks_dns_service_ip" {
  description = "AKS DNS service IP"
  type        = string
  default     = "10.0.0.10"
}

variable "aks_docker_bridge_cidr" {
  description = "AKS Docker bridge CIDR"
  type        = string
  default     = "172.17.0.1/16"
}

variable "enable_azure_ad_integration" {
  description = "Enable Azure AD integration"
  type        = bool
  default     = true
}

variable "aks_admin_group_object_ids" {
  description = "Azure AD group object IDs for AKS admins"
  type        = list(string)
  default     = []
}

variable "enable_key_vault_secrets_provider" {
  description = "Enable Key Vault secrets provider addon"
  type        = bool
  default     = true
}

variable "enable_open_service_mesh" {
  description = "Enable Open Service Mesh addon"
  type        = bool
  default     = false
}

variable "log_analytics_workspace_id" {
  description = "Log Analytics workspace ID"
  type        = string
  default     = ""
}

variable "enable_http_app_routing" {
  description = "Enable HTTP application routing (not recommended for production)"
  type        = bool
  default     = false
}

variable "aks_user_node_pools" {
  description = "AKS user node pools configuration"
  type = list(object({
    name          = string
    vm_size       = string
    node_count    = number
    min_count     = number
    max_count     = number
    priority      = string
    spot_max_price = optional(number)
    labels        = map(string)
    taints        = list(string)
  }))
  default = [
    {
      name       = "workload"
      vm_size    = "Standard_D4s_v3"
      node_count = 3
      min_count  = 1
      max_count  = 10
      priority   = "Regular"
      labels = {
        role = "workload"
      }
      taints = []
    },
    {
      name           = "spot"
      vm_size        = "Standard_D4s_v3"
      node_count     = 2
      min_count      = 0
      max_count      = 10
      priority       = "Spot"
      spot_max_price = 0.1
      labels = {
        role = "spot-workload"
      }
      taints = ["spot=true:NoSchedule"]
    }
  ]
}

# ============================================================================
# GCP GKE CONFIGURATION
# ============================================================================

variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
  default     = ""
}

variable "gcp_region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "gcp_network" {
  description = "GCP network name"
  type        = string
  default     = "default"
}

variable "gcp_subnetwork" {
  description = "GCP subnetwork name"
  type        = string
  default     = "default"
}

variable "gcp_pod_range_name" {
  description = "GCP pod IP range name"
  type        = string
  default     = "pod-range"
}

variable "gcp_service_range_name" {
  description = "GCP service IP range name"
  type        = string
  default     = "service-range"
}

variable "gke_autopilot_enabled" {
  description = "Enable GKE Autopilot mode"
  type        = bool
  default     = false
}

variable "gke_private_endpoint" {
  description = "Enable private endpoint for GKE master"
  type        = bool
  default     = true
}

variable "gke_master_ipv4_cidr" {
  description = "GKE master IPv4 CIDR block"
  type        = string
  default     = "172.16.0.0/28"
}

variable "gke_master_global_access" {
  description = "Enable global access to GKE master"
  type        = bool
  default     = true
}

variable "gke_authorized_networks" {
  description = "Authorized networks for GKE master access"
  type = list(object({
    cidr_block   = string
    display_name = string
  }))
  default = []
}

variable "gke_network_policy_enabled" {
  description = "Enable network policy for GKE"
  type        = bool
  default     = true
}

variable "gke_config_connector_enabled" {
  description = "Enable Config Connector addon"
  type        = bool
  default     = false
}

variable "gke_binary_authorization_enabled" {
  description = "Enable Binary Authorization"
  type        = bool
  default     = false
}

variable "gke_database_encryption_enabled" {
  description = "Enable database encryption at rest"
  type        = bool
  default     = true
}

variable "gke_cluster_autoscaling_enabled" {
  description = "Enable cluster-level autoscaling"
  type        = bool
  default     = true
}

variable "gke_autoscaling_resource_limits" {
  description = "Resource limits for cluster autoscaling"
  type = list(object({
    resource_type = string
    minimum       = number
    maximum       = number
  }))
  default = [
    {
      resource_type = "cpu"
      minimum       = 1
      maximum       = 100
    },
    {
      resource_type = "memory"
      minimum       = 1
      maximum       = 1000
    }
  ]
}

variable "gke_auto_provisioning_disk_size" {
  description = "Auto-provisioned node disk size"
  type        = number
  default     = 100
}

variable "gke_auto_provisioning_disk_type" {
  description = "Auto-provisioned node disk type"
  type        = string
  default     = "pd-standard"
}

variable "gke_auto_provisioning_image_type" {
  description = "Auto-provisioned node image type"
  type        = string
  default     = "COS_CONTAINERD"
}

variable "gke_managed_prometheus_enabled" {
  description = "Enable managed Prometheus"
  type        = bool
  default     = true
}

variable "gke_maintenance_start_time" {
  description = "Maintenance window start time"
  type        = string
  default     = "03:00"
}

variable "gke_node_pools" {
  description = "GKE node pools configuration"
  type = list(object({
    name                 = string
    machine_type         = string
    initial_node_count   = number
    min_node_count       = number
    max_node_count       = number
    total_min_node_count = optional(number)
    total_max_node_count = optional(number)
    location_policy      = string
    preemptible          = bool
    spot                 = bool
    image_type           = string
    disk_type            = string
    disk_size_gb         = number
    local_ssd_count      = number
    labels               = map(string)
    network_tags         = list(string)
    auto_upgrade         = bool
    max_surge            = number
    max_unavailable      = number
    pod_ipv4_cidr_block  = optional(string)
    taints = list(object({
      key    = string
      value  = string
      effect = string
    }))
  }))
  default = [
    {
      name                 = "system"
      machine_type         = "e2-medium"
      initial_node_count   = 1
      min_node_count       = 1
      max_node_count       = 3
      location_policy      = "BALANCED"
      preemptible          = false
      spot                 = false
      image_type           = "COS_CONTAINERD"
      disk_type            = "pd-standard"
      disk_size_gb         = 30
      local_ssd_count      = 0
      labels = {
        role = "system"
      }
      network_tags = ["gke-node"]
      auto_upgrade = true
      max_surge    = 1
      max_unavailable = 0
      taints = []
    },
    {
      name                 = "workload"
      machine_type         = "e2-standard-4"
      initial_node_count   = 2
      min_node_count       = 1
      max_node_count       = 10
      location_policy      = "BALANCED"
      preemptible          = false
      spot                 = true
      image_type           = "COS_CONTAINERD"
      disk_type            = "pd-ssd"
      disk_size_gb         = 100
      local_ssd_count      = 0
      labels = {
        role = "workload"
      }
      network_tags = ["gke-node", "workload"]
      auto_upgrade = true
      max_surge    = 1
      max_unavailable = 0
      taints = []
    }
  ]
}

variable "gke_node_service_account_roles" {
  description = "IAM roles for GKE node service account"
  type        = list(string)
  default = [
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/storage.objectViewer"
  ]
}

# ============================================================================
# SERVICE MESH CONFIGURATION
# ============================================================================

variable "enable_istio" {
  description = "Enable Istio service mesh"
  type        = bool
  default     = false
}

variable "istio_version" {
  description = "Istio version"
  type        = string
  default     = "1.19.0"
}

variable "istio_installation_method" {
  description = "Istio installation method (helm or istioctl)"
  type        = string
  default     = "helm"
}

variable "istio_trace_sampling" {
  description = "Istio trace sampling percentage"
  type        = number
  default     = 1.0
}

variable "istio_pilot_resources" {
  description = "Istio pilot resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "500m"
      memory = "2Gi"
    }
    limits = {
      cpu    = "1000m"
      memory = "4Gi"
    }
  }
}

variable "istio_proxy_resources" {
  description = "Istio proxy resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
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

variable "enable_istio_gateway" {
  description = "Enable Istio ingress gateway"
  type        = bool
  default     = true
}

variable "istio_gateway_service_type" {
  description = "Istio gateway service type"
  type        = string
  default     = "LoadBalancer"
}

variable "istio_gateway_annotations" {
  description = "Istio gateway service annotations"
  type        = map(string)
  default     = {}
}

variable "istio_gateway_resources" {
  description = "Istio gateway resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "100m"
      memory = "128Mi"
    }
    limits = {
      cpu    = "2000m"
      memory = "1024Mi"
    }
  }
}

variable "istio_gateway_autoscaling" {
  description = "Istio gateway autoscaling configuration"
  type = object({
    enabled                        = bool
    minReplicas                    = number
    maxReplicas                    = number
    targetCPUUtilizationPercentage = number
  })
  default = {
    enabled                        = true
    minReplicas                    = 1
    maxReplicas                    = 5
    targetCPUUtilizationPercentage = 80
  }
}

variable "enable_linkerd" {
  description = "Enable Linkerd service mesh"
  type        = bool
  default     = false
}

variable "linkerd_version" {
  description = "Linkerd version"
  type        = string
  default     = "stable-2.14.0"
}

variable "linkerd_log_level" {
  description = "Linkerd log level"
  type        = string
  default     = "info"
}

variable "linkerd_controller_resources" {
  description = "Linkerd controller resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "100m"
      memory = "50Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "250Mi"
    }
  }
}

variable "linkerd_proxy_resources" {
  description = "Linkerd proxy resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "100m"
      memory = "20Mi"
    }
    limits = {
      cpu    = "1000m"
      memory = "250Mi"
    }
  }
}

# ============================================================================
# MONITORING AND OBSERVABILITY
# ============================================================================

variable "enable_monitoring" {
  description = "Enable monitoring stack (Prometheus, Grafana, AlertManager)"
  type        = bool
  default     = true
}

variable "prometheus_operator_version" {
  description = "Prometheus operator version"
  type        = string
  default     = "51.2.0"
}

variable "prometheus_retention" {
  description = "Prometheus data retention period"
  type        = string
  default     = "30d"
}

variable "prometheus_storage_size" {
  description = "Prometheus storage size"
  type        = string
  default     = "50Gi"
}

variable "prometheus_resources" {
  description = "Prometheus resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
    requests = {
      cpu    = "500m"
      memory = "2Gi"
    }
    limits = {
      cpu    = "2000m"
      memory = "8Gi"
    }
  }
}

variable "grafana_admin_password" {
  description = "Grafana admin password"
  type        = string
  sensitive   = true
  default     = "admin"
}

variable "grafana_resources" {
  description = "Grafana resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
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

variable "grafana_storage_size" {
  description = "Grafana storage size"
  type        = string
  default     = "10Gi"
}

variable "grafana_dashboards" {
  description = "Grafana dashboards configuration"
  type        = map(string)
  default     = {}
}

variable "alertmanager_resources" {
  description = "AlertManager resource requirements"
  type = object({
    requests = object({
      cpu    = string
      memory = string
    })
    limits = object({
      cpu    = string
      memory = string
    })
  })
  default = {
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

variable "alertmanager_storage_size" {
  description = "AlertManager storage size"
  type        = string
  default     = "10Gi"
}

variable "monitoring_storage_class" {
  description = "Storage class for monitoring components"
  type        = string
  default     = "gp2"
}

# ============================================================================
# TAGS AND LABELS
# ============================================================================

variable "tags" {
  description = "Resource tags"
  type        = map(string)
  default = {
    ManagedBy   = "Terraform"
    Environment = "development"
  }
}