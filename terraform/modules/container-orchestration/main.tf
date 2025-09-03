# Enterprise Container Orchestration Module
# Provides multi-cloud Kubernetes deployment with service mesh integration
# Features: Auto-scaling, security policies, monitoring, and disaster recovery

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
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
  required_version = ">= 1.6.0"
}

# ============================================================================
# LOCAL VARIABLES
# ============================================================================

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  
  common_labels = {
    "app.kubernetes.io/managed-by" = "terraform"
    "app.kubernetes.io/part-of"   = var.project_name
    "environment"                 = var.environment
    "cost-center"                = var.cost_center
    "business-unit"              = var.business_unit
  }
  
  # Service mesh configuration
  service_mesh_config = {
    istio = {
      enabled = var.enable_istio
      version = var.istio_version
    }
    linkerd = {
      enabled = var.enable_linkerd
      version = var.linkerd_version
    }
  }
}

# ============================================================================
# AWS EKS CLUSTER WITH ADVANCED FEATURES
# ============================================================================

resource "aws_eks_cluster" "main" {
  count = var.deploy_to_aws ? 1 : 0
  
  name     = "${local.name_prefix}-eks"
  role_arn = aws_iam_role.eks_cluster[0].arn
  version  = var.kubernetes_version
  
  vpc_config {
    subnet_ids              = var.aws_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = var.eks_endpoint_public_access
    public_access_cidrs    = var.eks_public_access_cidrs
    security_group_ids     = [aws_security_group.eks_cluster[0].id]
  }
  
  encryption_config {
    provider {
      key_arn = aws_kms_key.eks_encryption[0].arn
    }
    resources = ["secrets"]
  }
  
  enabled_cluster_log_types = [
    "api",
    "audit", 
    "authenticator",
    "controllerManager",
    "scheduler"
  ]
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_cluster_policy,
    aws_iam_role_policy_attachment.eks_service_policy,
    aws_cloudwatch_log_group.eks_cluster
  ]
  
  tags = merge(var.tags, local.common_labels)
}

# EKS Node Groups with Mixed Instance Types
resource "aws_eks_node_group" "main" {
  count = var.deploy_to_aws ? length(var.eks_node_groups) : 0
  
  cluster_name    = aws_eks_cluster.main[0].name
  node_group_name = "${local.name_prefix}-${var.eks_node_groups[count.index].name}"
  node_role_arn   = aws_iam_role.eks_node_group[0].arn
  subnet_ids      = var.aws_subnet_ids
  
  capacity_type  = var.eks_node_groups[count.index].capacity_type
  instance_types = var.eks_node_groups[count.index].instance_types
  ami_type      = var.eks_node_groups[count.index].ami_type
  disk_size     = var.eks_node_groups[count.index].disk_size
  
  scaling_config {
    desired_size = var.eks_node_groups[count.index].desired_size
    max_size     = var.eks_node_groups[count.index].max_size
    min_size     = var.eks_node_groups[count.index].min_size
  }
  
  update_config {
    max_unavailable_percentage = 25
  }
  
  # Taints for specialized workloads
  dynamic "taint" {
    for_each = var.eks_node_groups[count.index].taints
    content {
      key    = taint.value.key
      value  = taint.value.value
      effect = taint.value.effect
    }
  }
  
  labels = merge(
    local.common_labels,
    var.eks_node_groups[count.index].labels
  )
  
  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node_policy,
    aws_iam_role_policy_attachment.eks_cni_policy,
    aws_iam_role_policy_attachment.eks_container_registry_policy
  ]
  
  tags = var.tags
}

# EKS Fargate Profiles for Serverless Workloads
resource "aws_eks_fargate_profile" "main" {
  count = var.deploy_to_aws && var.enable_fargate ? 1 : 0
  
  cluster_name           = aws_eks_cluster.main[0].name
  fargate_profile_name   = "${local.name_prefix}-fargate"
  pod_execution_role_arn = aws_iam_role.eks_fargate_pod[0].arn
  subnet_ids            = var.aws_private_subnet_ids
  
  selector {
    namespace = "fargate"
    labels = {
      "compute-type" = "fargate"
    }
  }
  
  selector {
    namespace = "kube-system"
    labels = {
      "k8s-app" = "kube-dns"
    }
  }
  
  tags = var.tags
}

# KMS Key for EKS Encryption
resource "aws_kms_key" "eks_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  description             = "KMS key for EKS encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-eks-encryption"
  })
}

resource "aws_kms_alias" "eks_encryption" {
  count = var.deploy_to_aws ? 1 : 0
  
  name          = "alias/${local.name_prefix}-eks-encryption"
  target_key_id = aws_kms_key.eks_encryption[0].key_id
}

# CloudWatch Log Group for EKS
resource "aws_cloudwatch_log_group" "eks_cluster" {
  count = var.deploy_to_aws ? 1 : 0
  
  name              = "/aws/eks/${local.name_prefix}/cluster"
  retention_in_days = var.log_retention_days
  kms_key_id       = aws_kms_key.eks_encryption[0].arn
  
  tags = var.tags
}

# ============================================================================
# AZURE AKS CLUSTER WITH ADVANCED FEATURES
# ============================================================================

resource "azurerm_kubernetes_cluster" "main" {
  count = var.deploy_to_azure ? 1 : 0
  
  name                = "${local.name_prefix}-aks"
  location            = var.azure_location
  resource_group_name = var.azure_resource_group_name
  dns_prefix          = "${local.name_prefix}-aks"
  kubernetes_version  = var.kubernetes_version
  
  # System node pool
  default_node_pool {
    name                         = "system"
    node_count                   = var.aks_system_node_count
    vm_size                     = var.aks_system_vm_size
    type                        = "VirtualMachineScaleSets"
    availability_zones          = var.azure_availability_zones
    enable_auto_scaling         = true
    min_count                   = var.aks_system_min_count
    max_count                   = var.aks_system_max_count
    max_pods                    = var.aks_max_pods_per_node
    os_disk_size_gb            = var.aks_os_disk_size
    vnet_subnet_id             = var.azure_subnet_id
    enable_node_public_ip      = false
    only_critical_addons_enabled = true
    
    upgrade_settings {
      max_surge = "33%"
    }
    
    tags = var.tags
  }
  
  # Identity configuration
  identity {
    type = "SystemAssigned"
  }
  
  # Network configuration
  network_profile {
    network_plugin    = var.aks_network_plugin
    network_policy    = var.aks_network_policy
    load_balancer_sku = "standard"
    outbound_type     = "loadBalancer"
    
    service_cidr       = var.aks_service_cidr
    dns_service_ip     = var.aks_dns_service_ip
    docker_bridge_cidr = var.aks_docker_bridge_cidr
  }
  
  # Azure Active Directory integration
  dynamic "azure_active_directory_role_based_access_control" {
    for_each = var.enable_azure_ad_integration ? [1] : []
    content {
      managed                = true
      tenant_id              = var.azure_tenant_id
      admin_group_object_ids = var.aks_admin_group_object_ids
      azure_rbac_enabled     = true
    }
  }
  
  # Key Vault secrets provider
  dynamic "key_vault_secrets_provider" {
    for_each = var.enable_key_vault_secrets_provider ? [1] : []
    content {
      secret_rotation_enabled  = true
      secret_rotation_interval = "2m"
    }
  }
  
  # Open Service Mesh addon
  dynamic "service_mesh_profile" {
    for_each = var.enable_open_service_mesh ? [1] : []
    content {
      mode                             = "Istio"
      internal_ingress_gateway_enabled = true
      external_ingress_gateway_enabled = true
    }
  }
  
  # Monitoring addon
  oms_agent {
    log_analytics_workspace_id = var.log_analytics_workspace_id
  }
  
  # HTTP application routing (disabled for production)
  http_application_routing_enabled = var.environment != "prod" && var.enable_http_app_routing
  
  # Auto-scaler profile
  auto_scaler_profile {
    balance_similar_node_groups      = true
    expander                        = "priority"
    max_graceful_termination_sec    = "600"
    max_node_provisioning_time      = "15m"
    max_unready_nodes               = 3
    max_unready_percentage          = 45
    new_pod_scale_up_delay          = "10s"
    scale_down_delay_after_add      = "10m"
    scale_down_delay_after_delete   = "10s"
    scale_down_delay_after_failure  = "3m"
    scale_down_unneeded             = "10m"
    scale_down_unready              = "20m"
    scale_down_utilization_threshold = "0.5"
    empty_bulk_delete_max           = "10"
    skip_nodes_with_local_storage   = true
    skip_nodes_with_system_pods     = true
  }
  
  tags = merge(var.tags, local.common_labels)
}

# AKS User Node Pools
resource "azurerm_kubernetes_cluster_node_pool" "user_pools" {
  count = var.deploy_to_azure ? length(var.aks_user_node_pools) : 0
  
  name                  = var.aks_user_node_pools[count.index].name
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main[0].id
  vm_size              = var.aks_user_node_pools[count.index].vm_size
  node_count           = var.aks_user_node_pools[count.index].node_count
  availability_zones   = var.azure_availability_zones
  enable_auto_scaling  = true
  min_count           = var.aks_user_node_pools[count.index].min_count
  max_count           = var.aks_user_node_pools[count.index].max_count
  max_pods            = var.aks_max_pods_per_node
  os_disk_size_gb     = var.aks_os_disk_size
  vnet_subnet_id      = var.azure_subnet_id
  enable_node_public_ip = false
  
  # Spot instances for cost optimization
  priority        = var.aks_user_node_pools[count.index].priority
  eviction_policy = var.aks_user_node_pools[count.index].priority == "Spot" ? "Delete" : null
  spot_max_price  = var.aks_user_node_pools[count.index].priority == "Spot" ? var.aks_user_node_pools[count.index].spot_max_price : null
  
  # Node taints
  dynamic "node_taints" {
    for_each = var.aks_user_node_pools[count.index].taints
    content {
      node_taints.value
    }
  }
  
  # Node labels
  node_labels = merge(
    local.common_labels,
    var.aks_user_node_pools[count.index].labels
  )
  
  upgrade_settings {
    max_surge = "33%"
  }
  
  tags = var.tags
}

# ============================================================================
# GCP GKE CLUSTER WITH ADVANCED FEATURES
# ============================================================================

resource "google_container_cluster" "main" {
  count = var.deploy_to_gcp ? 1 : 0
  
  name     = "${local.name_prefix}-gke"
  location = var.gcp_region
  project  = var.gcp_project_id
  
  # Autopilot or Standard mode
  enable_autopilot = var.gke_autopilot_enabled
  
  # Remove default node pool
  remove_default_node_pool = !var.gke_autopilot_enabled
  initial_node_count       = var.gke_autopilot_enabled ? null : 1
  
  # Network configuration
  network    = var.gcp_network
  subnetwork = var.gcp_subnetwork
  
  # IP allocation policy
  ip_allocation_policy {
    cluster_secondary_range_name  = var.gcp_pod_range_name
    services_secondary_range_name = var.gcp_service_range_name
  }
  
  # Private cluster configuration
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = var.gke_private_endpoint
    master_ipv4_cidr_block = var.gke_master_ipv4_cidr
    
    master_global_access_config {
      enabled = var.gke_master_global_access
    }
  }
  
  # Master authorized networks
  dynamic "master_authorized_networks_config" {
    for_each = var.gke_authorized_networks
    content {
      cidr_blocks {
        cidr_block   = master_authorized_networks_config.value.cidr_block
        display_name = master_authorized_networks_config.value.display_name
      }
    }
  }
  
  # Workload Identity
  workload_identity_config {
    workload_pool = "${var.gcp_project_id}.svc.id.goog"
  }
  
  # Network policy
  network_policy {
    enabled  = var.gke_network_policy_enabled
    provider = var.gke_network_policy_enabled ? "CALICO" : null
  }
  
  # Addons
  addons_config {
    http_load_balancing {
      disabled = false
    }
    
    horizontal_pod_autoscaling {
      disabled = false
    }
    
    network_policy_config {
      disabled = !var.gke_network_policy_enabled
    }
    
    dns_cache_config {
      enabled = true
    }
    
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
    
    config_connector_config {
      enabled = var.gke_config_connector_enabled
    }
    
    istio_config {
      disabled = !var.enable_istio
      auth     = var.enable_istio ? "AUTH_MUTUAL_TLS" : null
    }
  }
  
  # Binary authorization
  binary_authorization {
    enabled = var.gke_binary_authorization_enabled
    evaluation_mode = var.gke_binary_authorization_enabled ? "PROJECT_SINGLETON_POLICY_ENFORCE" : "DISABLED"
  }
  
  # Database encryption
  database_encryption {
    state    = var.gke_database_encryption_enabled ? "ENCRYPTED" : "DECRYPTED"
    key_name = var.gke_database_encryption_enabled ? google_kms_crypto_key.gke_encryption[0].id : null
  }
  
  # Cluster autoscaling
  cluster_autoscaling {
    enabled = var.gke_cluster_autoscaling_enabled
    
    dynamic "resource_limits" {
      for_each = var.gke_cluster_autoscaling_enabled ? var.gke_autoscaling_resource_limits : []
      content {
        resource_type = resource_limits.value.resource_type
        minimum       = resource_limits.value.minimum
        maximum       = resource_limits.value.maximum
      }
    }
    
    dynamic "auto_provisioning_defaults" {
      for_each = var.gke_cluster_autoscaling_enabled ? [1] : []
      content {
        oauth_scopes = [
          "https://www.googleapis.com/auth/cloud-platform"
        ]
        
        service_account = google_service_account.gke_node_service_account[0].email
        
        disk_size    = var.gke_auto_provisioning_disk_size
        disk_type    = var.gke_auto_provisioning_disk_type
        image_type   = var.gke_auto_provisioning_image_type
        
        boot_disk_kms_key = var.gke_database_encryption_enabled ? google_kms_crypto_key.gke_encryption[0].id : null
        
        shielded_instance_config {
          enable_secure_boot          = true
          enable_integrity_monitoring = true
        }
      }
    }
  }
  
  # Logging configuration
  logging_config {
    enable_components = [
      "SYSTEM_COMPONENTS",
      "WORKLOADS",
      "API_SERVER"
    ]
  }
  
  # Monitoring configuration
  monitoring_config {
    enable_components = [
      "SYSTEM_COMPONENTS",
      "WORKLOADS",
      "API_SERVER",
      "CONTROLLER_MANAGER",
      "SCHEDULER"
    ]
    
    managed_prometheus {
      enabled = var.gke_managed_prometheus_enabled
    }
  }
  
  # Maintenance policy
  maintenance_policy {
    daily_maintenance_window {
      start_time = var.gke_maintenance_start_time
    }
  }
  
  # Resource labels
  resource_labels = merge(var.tags, local.common_labels)
}

# GKE Node Pools
resource "google_container_node_pool" "main" {
  count = var.deploy_to_gcp && !var.gke_autopilot_enabled ? length(var.gke_node_pools) : 0
  
  name     = var.gke_node_pools[count.index].name
  location = var.gcp_region
  cluster  = google_container_cluster.main[0].name
  project  = var.gcp_project_id
  
  node_count = var.gke_node_pools[count.index].initial_node_count
  
  # Autoscaling configuration
  autoscaling {
    min_node_count       = var.gke_node_pools[count.index].min_node_count
    max_node_count       = var.gke_node_pools[count.index].max_node_count
    location_policy      = var.gke_node_pools[count.index].location_policy
    total_min_node_count = var.gke_node_pools[count.index].total_min_node_count
    total_max_node_count = var.gke_node_pools[count.index].total_max_node_count
  }
  
  # Node configuration
  node_config {
    preemptible     = var.gke_node_pools[count.index].preemptible
    spot            = var.gke_node_pools[count.index].spot
    machine_type    = var.gke_node_pools[count.index].machine_type
    image_type      = var.gke_node_pools[count.index].image_type
    disk_type       = var.gke_node_pools[count.index].disk_type
    disk_size_gb    = var.gke_node_pools[count.index].disk_size_gb
    local_ssd_count = var.gke_node_pools[count.index].local_ssd_count
    
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
    
    service_account = google_service_account.gke_node_service_account[0].email
    
    labels = merge(
      local.common_labels,
      var.gke_node_pools[count.index].labels
    )
    
    tags = var.gke_node_pools[count.index].network_tags
    
    # Taints
    dynamic "taint" {
      for_each = var.gke_node_pools[count.index].taints
      content {
        key    = taint.value.key
        value  = taint.value.value
        effect = taint.value.effect
      }
    }
    
    # Shielded instance configuration
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }
    
    # Workload metadata configuration
    workload_metadata_config {
      mode = "GKE_METADATA"
    }
    
    boot_disk_kms_key = var.gke_database_encryption_enabled ? google_kms_crypto_key.gke_encryption[0].id : null
  }
  
  # Upgrade settings
  upgrade_settings {
    max_surge       = var.gke_node_pools[count.index].max_surge
    max_unavailable = var.gke_node_pools[count.index].max_unavailable
    
    strategy = "SURGE"
    
    blue_green_settings {
      standard_rollout_policy {
        batch_percentage    = 100
        batch_node_count    = var.gke_node_pools[count.index].initial_node_count
        batch_soak_duration = "300s"
      }
      node_pool_soak_duration = "1800s"
    }
  }
  
  # Management settings
  management {
    auto_repair  = true
    auto_upgrade = var.gke_node_pools[count.index].auto_upgrade
  }
  
  # Network configuration
  network_config {
    create_pod_range = false
    pod_range        = var.gcp_pod_range_name
    pod_ipv4_cidr_block = var.gke_node_pools[count.index].pod_ipv4_cidr_block
  }
}

# KMS Key for GKE Encryption
resource "google_kms_crypto_key" "gke_encryption" {
  count = var.deploy_to_gcp && var.gke_database_encryption_enabled ? 1 : 0
  
  name            = "${local.name_prefix}-gke-encryption"
  key_ring        = google_kms_key_ring.gke_encryption[0].id
  rotation_period = "7776000s"  # 90 days
  
  lifecycle {
    prevent_destroy = true
  }
  
  labels = var.tags
}

resource "google_kms_key_ring" "gke_encryption" {
  count = var.deploy_to_gcp && var.gke_database_encryption_enabled ? 1 : 0
  
  name     = "${local.name_prefix}-gke-encryption"
  location = var.gcp_region
  project  = var.gcp_project_id
}

# ============================================================================
# IAM ROLES AND POLICIES
# ============================================================================

# AWS EKS IAM Roles
resource "aws_iam_role" "eks_cluster" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-eks-cluster-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster[0].name
}

resource "aws_iam_role_policy_attachment" "eks_service_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSServicePolicy"
  role       = aws_iam_role.eks_cluster[0].name
}

# EKS Node Group IAM Role
resource "aws_iam_role" "eks_node_group" {
  count = var.deploy_to_aws ? 1 : 0
  
  name = "${local.name_prefix}-eks-node-group-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "eks_worker_node_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_node_group[0].name
}

resource "aws_iam_role_policy_attachment" "eks_cni_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_node_group[0].name
}

resource "aws_iam_role_policy_attachment" "eks_container_registry_policy" {
  count = var.deploy_to_aws ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_node_group[0].name
}

# EKS Fargate IAM Role
resource "aws_iam_role" "eks_fargate_pod" {
  count = var.deploy_to_aws && var.enable_fargate ? 1 : 0
  
  name = "${local.name_prefix}-eks-fargate-pod-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "eks-fargate-pods.amazonaws.com"
        }
      }
    ]
  })
  
  tags = var.tags
}

resource "aws_iam_role_policy_attachment" "eks_fargate_pod_policy" {
  count = var.deploy_to_aws && var.enable_fargate ? 1 : 0
  
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSFargatePodExecutionRolePolicy"
  role       = aws_iam_role.eks_fargate_pod[0].name
}

# GCP Service Accounts
resource "google_service_account" "gke_node_service_account" {
  count = var.deploy_to_gcp ? 1 : 0
  
  account_id   = "${local.name_prefix}-gke-node-sa"
  display_name = "GKE Node Service Account"
  project      = var.gcp_project_id
}

resource "google_project_iam_member" "gke_node_service_account" {
  count = var.deploy_to_gcp ? length(var.gke_node_service_account_roles) : 0
  
  project = var.gcp_project_id
  role    = var.gke_node_service_account_roles[count.index]
  member  = "serviceAccount:${google_service_account.gke_node_service_account[0].email}"
}

# ============================================================================
# SECURITY GROUPS AND FIREWALL RULES
# ============================================================================

# AWS Security Group for EKS
resource "aws_security_group" "eks_cluster" {
  count = var.deploy_to_aws ? 1 : 0
  
  name_prefix = "${local.name_prefix}-eks-cluster-"
  vpc_id      = var.aws_vpc_id
  
  ingress {
    from_port = 443
    to_port   = 443
    protocol  = "tcp"
    cidr_blocks = var.eks_api_server_access_cidrs
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = merge(var.tags, {
    Name = "${local.name_prefix}-eks-cluster-sg"
  })
}

# ============================================================================
# SERVICE MESH CONFIGURATION
# ============================================================================

# Istio Service Mesh
resource "helm_release" "istio_base" {
  count = var.enable_istio && var.istio_installation_method == "helm" ? 1 : 0
  
  name       = "istio-base"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "base"
  namespace  = "istio-system"
  version    = var.istio_version
  
  create_namespace = true
  
  values = [
    yamlencode({
      global = {
        istioNamespace = "istio-system"
      }
    })
  ]
  
  depends_on = [
    aws_eks_cluster.main,
    azurerm_kubernetes_cluster.main,
    google_container_cluster.main
  ]
}

resource "helm_release" "istio_istiod" {
  count = var.enable_istio && var.istio_installation_method == "helm" ? 1 : 0
  
  name       = "istiod"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "istiod"
  namespace  = "istio-system"
  version    = var.istio_version
  
  values = [
    yamlencode({
      telemetry = {
        v2 = {
          prometheus = {
            configOverride = {
              metric_relabeling_configs = [
                {
                  source_labels = ["__name__"]
                  regex = "istio_.*"
                  target_label = "__tmp_istio_metric"
                }
              ]
            }
          }
        }
      }
      pilot = {
        traceSampling = var.istio_trace_sampling
        resources = var.istio_pilot_resources
      }
      global = {
        proxy = {
          resources = var.istio_proxy_resources
        }
      }
    })
  ]
  
  depends_on = [helm_release.istio_base]
}

resource "helm_release" "istio_gateway" {
  count = var.enable_istio && var.enable_istio_gateway && var.istio_installation_method == "helm" ? 1 : 0
  
  name       = "istio-ingress"
  repository = "https://istio-release.storage.googleapis.com/charts"
  chart      = "gateway"
  namespace  = "istio-ingress"
  version    = var.istio_version
  
  create_namespace = true
  
  values = [
    yamlencode({
      service = {
        type = var.istio_gateway_service_type
        annotations = var.istio_gateway_annotations
      }
      resources = var.istio_gateway_resources
      autoscaling = var.istio_gateway_autoscaling
    })
  ]
  
  depends_on = [helm_release.istio_istiod]
}

# Linkerd Service Mesh
resource "helm_release" "linkerd_crds" {
  count = var.enable_linkerd ? 1 : 0
  
  name       = "linkerd-crds"
  repository = "https://helm.linkerd.io/stable"
  chart      = "linkerd-crds"
  namespace  = "linkerd"
  version    = var.linkerd_version
  
  create_namespace = true
  
  depends_on = [
    aws_eks_cluster.main,
    azurerm_kubernetes_cluster.main,
    google_container_cluster.main
  ]
}

resource "helm_release" "linkerd_control_plane" {
  count = var.enable_linkerd ? 1 : 0
  
  name       = "linkerd-control-plane"
  repository = "https://helm.linkerd.io/stable"
  chart      = "linkerd-control-plane"
  namespace  = "linkerd"
  version    = var.linkerd_version
  
  values = [
    yamlencode({
      controllerLogLevel = var.linkerd_log_level
      proxyLogLevel     = var.linkerd_log_level
      
      controllerResources = var.linkerd_controller_resources
      proxyResources     = var.linkerd_proxy_resources
      
      identity = {
        issuer = {
          scheme = "kubernetes.io/tls"
        }
      }
    })
  ]
  
  depends_on = [helm_release.linkerd_crds]
}

# ============================================================================
# MONITORING AND OBSERVABILITY
# ============================================================================

# Prometheus Operator
resource "helm_release" "prometheus_operator" {
  count = var.enable_monitoring ? 1 : 0
  
  name       = "prometheus-operator"
  repository = "https://prometheus-community.github.io/helm-charts"
  chart      = "kube-prometheus-stack"
  namespace  = "monitoring"
  version    = var.prometheus_operator_version
  
  create_namespace = true
  
  values = [
    yamlencode({
      prometheus = {
        prometheusSpec = {
          retention = var.prometheus_retention
          storageSpec = {
            volumeClaimTemplate = {
              spec = {
                storageClassName = var.monitoring_storage_class
                accessModes      = ["ReadWriteOnce"]
                resources = {
                  requests = {
                    storage = var.prometheus_storage_size
                  }
                }
              }
            }
          }
          resources = var.prometheus_resources
        }
      }
      
      grafana = {
        adminPassword = var.grafana_admin_password
        resources     = var.grafana_resources
        persistence = {
          enabled          = true
          storageClassName = var.monitoring_storage_class
          size             = var.grafana_storage_size
        }
        
        dashboardProviders = {
          dashboardproviders.yaml = {
            apiVersion = 1
            providers = [
              {
                name    = "default"
                orgId   = 1
                folder  = ""
                type    = "file"
                options = {
                  path = "/var/lib/grafana/dashboards/default"
                }
              }
            ]
          }
        }
        
        dashboards = {
          default = var.grafana_dashboards
        }
      }
      
      alertmanager = {
        alertmanagerSpec = {
          resources = var.alertmanager_resources
          storage = {
            volumeClaimTemplate = {
              spec = {
                storageClassName = var.monitoring_storage_class
                accessModes      = ["ReadWriteOnce"]
                resources = {
                  requests = {
                    storage = var.alertmanager_storage_size
                  }
                }
              }
            }
          }
        }
      }
    })
  ]
  
  depends_on = [
    aws_eks_cluster.main,
    azurerm_kubernetes_cluster.main,
    google_container_cluster.main
  ]
}