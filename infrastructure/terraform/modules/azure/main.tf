# Azure Cloud Infrastructure Module
# Enterprise-grade Azure resources for data platform

terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.70"
    }
  }
}

# ============================================================================
# RESOURCE GROUP
# ============================================================================

resource "azurerm_resource_group" "main" {
  name     = "${var.resource_prefix}-rg"
  location = var.location

  tags = var.common_tags
}

# ============================================================================
# VIRTUAL NETWORK AND NETWORKING
# ============================================================================

resource "azurerm_virtual_network" "main" {
  name                = "${var.resource_prefix}-vnet"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = var.vnet_address_space

  tags = var.common_tags
}

# Public subnet for load balancers and gateways
resource "azurerm_subnet" "public" {
  count = length(var.availability_zones)

  name                 = "${var.resource_prefix}-public-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, count.index)]
}

# Private subnet for AKS nodes and applications
resource "azurerm_subnet" "private" {
  count = length(var.availability_zones)

  name                 = "${var.resource_prefix}-private-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, count.index + 10)]
}

# Data subnet for databases and data services
resource "azurerm_subnet" "data" {
  count = length(var.availability_zones)

  name                 = "${var.resource_prefix}-data-subnet-${count.index + 1}"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, count.index + 20)]
  
  # Delegate subnet for data services
  delegation {
    name = "data-services-delegation"
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
      ]
    }
  }
}

# Gateway subnet for VPN and ExpressRoute gateways
resource "azurerm_subnet" "gateway" {
  name                 = "GatewaySubnet"  # Must be exactly this name
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [cidrsubnet(var.vnet_address_space[0], 8, 30)]
}

# ============================================================================
# NETWORK SECURITY GROUPS
# ============================================================================

resource "azurerm_network_security_group" "aks_nodes" {
  name                = "${var.resource_prefix}-aks-nodes-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  # Allow inbound HTTP/HTTPS
  security_rule {
    name                       = "AllowHTTPS"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "AllowHTTP"
    priority                   = 101
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  # Allow Kubernetes API server
  security_rule {
    name                       = "AllowKubernetesAPI"
    priority                   = 102
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "6443"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  # Allow node-to-node communication
  security_rule {
    name                       = "AllowNodeCommunication"
    priority                   = 103
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "VirtualNetwork"
  }

  tags = var.common_tags
}

resource "azurerm_network_security_group" "data_services" {
  name                = "${var.resource_prefix}-data-services-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  # Allow PostgreSQL access from AKS nodes
  security_rule {
    name                       = "AllowPostgreSQL"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "5432"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  # Allow Synapse access
  security_rule {
    name                       = "AllowSynapseSQL"
    priority                   = 101
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "1433"
    source_address_prefix      = "VirtualNetwork"
    destination_address_prefix = "*"
  }

  tags = var.common_tags
}

# Associate NSGs with subnets
resource "azurerm_subnet_network_security_group_association" "private" {
  count = length(azurerm_subnet.private)

  subnet_id                 = azurerm_subnet.private[count.index].id
  network_security_group_id = azurerm_network_security_group.aks_nodes.id
}

resource "azurerm_subnet_network_security_group_association" "data" {
  count = length(azurerm_subnet.data)

  subnet_id                 = azurerm_subnet.data[count.index].id
  network_security_group_id = azurerm_network_security_group.data_services.id
}

# ============================================================================
# KEY VAULT FOR ENCRYPTION AND SECRETS
# ============================================================================

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "${var.resource_prefix}-kv-${random_string.kv_suffix.result}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  enabled_for_disk_encryption     = true
  enabled_for_deployment          = true
  enabled_for_template_deployment = true
  purge_protection_enabled        = var.environment == "production"
  soft_delete_retention_days      = var.environment == "production" ? 90 : 7

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get", "List", "Create", "Delete", "Update", "Import", "Backup", "Restore", "Recover"
    ]

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Recover", "Backup", "Restore"
    ]

    certificate_permissions = [
      "Get", "List", "Create", "Delete", "Update", "Import", "ManageContacts", "ManageIssuers"
    ]
  }

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
    virtual_network_subnet_ids = concat(
      azurerm_subnet.private[*].id,
      azurerm_subnet.data[*].id
    )
  }

  tags = var.common_tags
}

resource "random_string" "kv_suffix" {
  length  = 4
  special = false
  upper   = false
}

# ============================================================================
# AKS CLUSTER
# ============================================================================

resource "azurerm_kubernetes_cluster" "main" {
  name                = "${var.resource_prefix}-aks"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = "${var.resource_prefix}-aks"
  kubernetes_version  = var.kubernetes_version

  # System node pool
  default_node_pool {
    name                = "system"
    node_count         = var.env_config.high_availability ? 3 : 1
    vm_size            = local.system_vm_sizes[var.env_config.cluster_size]
    vnet_subnet_id     = azurerm_subnet.private[0].id
    zones              = var.env_config.high_availability ? ["1", "2", "3"] : ["1"]
    
    enable_auto_scaling = var.env_config.auto_scaling
    min_count          = var.env_config.auto_scaling ? 1 : null
    max_count          = var.env_config.auto_scaling ? local.max_node_counts[var.env_config.cluster_size] : null
    
    enable_host_encryption = true
    enable_node_public_ip  = false
    max_pods              = 50
    os_disk_type          = "Managed"
    os_disk_size_gb       = var.env_config.cluster_size == "large" ? 100 : 50
    
    upgrade_settings {
      max_surge = "33%"
    }

    tags = var.common_tags
  }

  # Identity configuration
  identity {
    type = "SystemAssigned"
  }

  # Network configuration
  network_profile {
    network_plugin     = "azure"
    network_policy     = "azure"
    load_balancer_sku  = "standard"
    outbound_type      = "loadBalancer"
    dns_service_ip     = "10.2.0.10"
    docker_bridge_cidr = "172.17.0.1/16"
    service_cidr       = "10.2.0.0/24"
  }

  # Azure AD integration
  azure_active_directory_role_based_access_control {
    managed            = true
    azure_rbac_enabled = true
    admin_group_object_ids = var.aks_admin_group_object_ids
  }

  # Add-ons
  oms_agent {
    log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  }

  microsoft_defender {
    log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  }

  key_vault_secrets_provider {
    secret_rotation_enabled  = true
    secret_rotation_interval = "2m"
  }

  # Private cluster configuration for production
  private_cluster_enabled             = var.environment == "production"
  private_dns_zone_id                = var.environment == "production" ? azurerm_private_dns_zone.aks[0].id : null
  private_cluster_public_fqdn_enabled = false

  # Auto-scaler configuration
  auto_scaler_profile {
    balance_similar_node_groups      = true
    expander                        = "random"
    max_graceful_termination_sec    = "600"
    max_node_provisioning_time      = "15m"
    max_unready_nodes               = 3
    max_unready_percentage          = 45
    new_pod_scale_up_delay          = "10s"
    scale_down_delay_after_add      = "10m"
    scale_down_delay_after_delete   = "10s"
    scale_down_delay_after_failure  = "3m"
    scan_interval                   = "10s"
    scale_down_unneeded             = "10m"
    scale_down_unready              = "20m"
    scale_down_utilization_threshold = "0.5"
  }

  # Monitoring
  oms_agent {
    log_analytics_workspace_id      = azurerm_log_analytics_workspace.main.id
    msi_auth_for_monitoring_enabled = true
  }

  tags = var.common_tags
}

# Private DNS Zone for AKS (production only)
resource "azurerm_private_dns_zone" "aks" {
  count = var.environment == "production" ? 1 : 0
  
  name                = "${var.resource_prefix}-aks.private.zone"
  resource_group_name = azurerm_resource_group.main.name

  tags = var.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "aks" {
  count = var.environment == "production" ? 1 : 0
  
  name                  = "${var.resource_prefix}-aks-dns-link"
  resource_group_name   = azurerm_resource_group.main.name
  private_dns_zone_name = azurerm_private_dns_zone.aks[0].name
  virtual_network_id    = azurerm_virtual_network.main.id
  registration_enabled  = false

  tags = var.common_tags
}

# User node pool for data processing workloads
resource "azurerm_kubernetes_cluster_node_pool" "user" {
  name                  = "user"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.main.id
  vm_size              = local.user_vm_sizes[var.env_config.cluster_size]
  node_count           = local.user_node_counts[var.env_config.cluster_size].desired
  vnet_subnet_id       = azurerm_subnet.private[1].id
  zones                = var.env_config.high_availability ? ["1", "2", "3"] : ["1"]

  enable_auto_scaling = var.env_config.auto_scaling
  min_count          = var.env_config.auto_scaling ? local.user_node_counts[var.env_config.cluster_size].min : null
  max_count          = var.env_config.auto_scaling ? local.user_node_counts[var.env_config.cluster_size].max : null
  
  enable_host_encryption = true
  enable_node_public_ip  = false
  max_pods              = 50
  os_disk_type          = "Managed"
  os_disk_size_gb       = var.env_config.cluster_size == "large" ? 200 : 100
  priority              = var.enable_cost_optimization ? "Spot" : "Regular"
  spot_max_price        = var.enable_cost_optimization ? 0.1 : null

  # Node labels for workload targeting
  node_labels = {
    "node-type" = "user"
    "workload"  = "data-processing"
  }

  # Node taints for dedicated workloads
  node_taints = var.env_config.cluster_size == "large" ? [
    "workload=data-processing:NoSchedule"
  ] : []

  upgrade_settings {
    max_surge = "33%"
  }

  tags = var.common_tags
}

# ============================================================================
# DATA STORAGE (AZURE DATA LAKE STORAGE GEN2)
# ============================================================================

resource "azurerm_storage_account" "data_lake" {
  name                     = "${lower(replace(var.resource_prefix, "-", ""))}dl${random_string.storage_suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = var.env_config.high_availability ? "GRS" : "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true  # Hierarchical namespace for Data Lake Gen2
  
  # Security configurations
  enable_https_traffic_only = true
  min_tls_version          = "TLS1_2"
  allow_nested_items_to_be_public = false

  # Network access rules
  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"]
    virtual_network_subnet_ids = concat(
      azurerm_subnet.private[*].id,
      azurerm_subnet.data[*].id
    )
  }

  # Blob properties for lifecycle management
  blob_properties {
    # Enable versioning
    versioning_enabled = true
    
    # Change feed
    change_feed_enabled = var.env_config.monitoring_level == "comprehensive"
    
    # Point-in-time restore
    restore_policy {
      days = var.env_config.backup_retention > 30 ? 30 : var.env_config.backup_retention
    }

    # Container delete retention
    container_delete_retention_policy {
      days = var.env_config.backup_retention
    }

    # Blob delete retention
    delete_retention_policy {
      days = var.env_config.backup_retention
    }
  }

  tags = var.common_tags
}

resource "random_string" "storage_suffix" {
  length  = 4
  special = false
  upper   = false
}

# Data Lake containers for different data layers
resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.data_lake.id

  properties = {
    "description" = "Raw data ingestion layer"
    "tier"        = "bronze"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.data_lake.id

  properties = {
    "description" = "Cleaned and validated data layer"
    "tier"        = "silver"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.data_lake.id

  properties = {
    "description" = "Curated and aggregated data layer"
    "tier"        = "gold"
  }
}

# Storage lifecycle management for cost optimization
resource "azurerm_storage_management_policy" "data_lake" {
  count = var.enable_cost_optimization ? 1 : 0

  storage_account_id = azurerm_storage_account.data_lake.id

  rule {
    name    = "bronze_lifecycle"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["bronze/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 30
        tier_to_archive_after_days_since_modification_greater_than = 90
        delete_after_days_since_modification_greater_than          = 2555  # 7 years
      }
      snapshot {
        delete_after_days_since_creation_greater_than = 30
      }
      version {
        delete_after_days_since_creation = 30
      }
    }
  }

  rule {
    name    = "silver_lifecycle"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["silver/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 60
        tier_to_archive_after_days_since_modification_greater_than = 180
        delete_after_days_since_modification_greater_than          = 2555  # 7 years
      }
    }
  }

  rule {
    name    = "gold_lifecycle"
    enabled = true
    filters {
      blob_types   = ["blockBlob"]
      prefix_match = ["gold/"]
    }
    actions {
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than    = 90
        tier_to_archive_after_days_since_modification_greater_than = 365
        delete_after_days_since_modification_greater_than          = 3650  # 10 years
      }
    }
  }
}

# ============================================================================
# EVENT HUBS FOR STREAMING DATA
# ============================================================================

resource "azurerm_eventhub_namespace" "main" {
  name                = "${var.resource_prefix}-eventhubs"
  resource_group_name = azurerm_resource_group.main.name
  location           = azurerm_resource_group.main.location
  sku                = var.env_config.cluster_size == "large" ? "Standard" : "Basic"
  capacity           = var.env_config.cluster_size == "large" ? 2 : 1
  
  # Auto-inflate for dynamic scaling
  auto_inflate_enabled     = var.env_config.cluster_size == "large"
  maximum_throughput_units = var.env_config.cluster_size == "large" ? 10 : null

  # Network security
  public_network_access_enabled = false
  network_rulesets = [{
    default_action                 = "Deny"
    public_network_access_enabled  = false
    trusted_service_access_enabled = true
    virtual_network_rule = [
      for subnet in azurerm_subnet.private : {
        subnet_id                                       = subnet.id
        ignore_missing_virtual_network_service_endpoint = false
      }
    ]
  }]

  tags = var.common_tags
}

# Event Hub for data streaming
resource "azurerm_eventhub" "data_stream" {
  name                = "data-stream"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = local.eventhub_partitions[var.env_config.cluster_size]
  message_retention   = var.env_config.backup_retention > 7 ? 7 : var.env_config.backup_retention

  # Capture to Data Lake for long-term storage
  capture_description {
    enabled             = true
    encoding            = "Avro"
    interval_in_seconds = 300
    size_limit_in_bytes = 104857600  # 100MB
    skip_empty_archives = true

    destination {
      name                = "EventHubArchive.AzureBlockBlob"
      archive_name_format = "{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
      blob_container_name = azurerm_storage_data_lake_gen2_filesystem.bronze.name
      storage_account_id  = azurerm_storage_account.data_lake.id
    }
  }
}

# Consumer groups for different applications
resource "azurerm_eventhub_consumer_group" "data_processing" {
  name                = "data-processing"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.data_stream.name
  resource_group_name = azurerm_resource_group.main.name
}

resource "azurerm_eventhub_consumer_group" "real_time_analytics" {
  name                = "real-time-analytics"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = azurerm_eventhub.data_stream.name
  resource_group_name = azurerm_resource_group.main.name
}

# ============================================================================
# SYNAPSE ANALYTICS WORKSPACE
# ============================================================================

resource "azurerm_synapse_workspace" "main" {
  count = var.env_config.cluster_size != "small" ? 1 : 0

  name                                 = "${var.resource_prefix}-synapse"
  resource_group_name                  = azurerm_resource_group.main.name
  location                            = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.silver.id

  sql_administrator_login          = "sqladminuser"
  sql_administrator_login_password = var.synapse_sql_password

  # Security configurations
  public_network_access_enabled = false
  managed_virtual_network_enabled = true
  
  # Data exfiltration protection
  data_exfiltration_protection_enabled = var.environment == "production"

  # Azure AD authentication
  aad_admin {
    login     = var.synapse_aad_admin_login
    object_id = var.synapse_aad_admin_object_id
    tenant_id = data.azurerm_client_config.current.tenant_id
  }

  # Identity
  identity {
    type = "SystemAssigned"
  }

  tags = var.common_tags
}

# Synapse SQL Pool for data warehousing
resource "azurerm_synapse_sql_pool" "main" {
  count = var.env_config.cluster_size == "large" ? 1 : 0

  name                 = "datawarehouse"
  synapse_workspace_id = azurerm_synapse_workspace.main[0].id
  sku_name            = "DW100c"
  create_mode         = "Default"

  # Auto-pause for cost optimization
  auto_pause {
    delay_in_minutes = var.enable_cost_optimization ? 60 : -1
  }

  # Auto-scale for performance
  auto_scale {
    max_size_gb = var.env_config.cluster_size == "large" ? 10240 : 5120
  }

  tags = var.common_tags
}

# Synapse Spark Pool for big data processing
resource "azurerm_synapse_spark_pool" "main" {
  count = var.env_config.cluster_size != "small" ? 1 : 0

  name                 = "sparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.main[0].id
  node_size_family     = "MemoryOptimized"
  node_size            = var.env_config.cluster_size == "large" ? "Large" : "Medium"

  # Auto-scaling configuration
  auto_scale {
    max_node_count = local.spark_max_nodes[var.env_config.cluster_size]
    min_node_count = 3
  }

  # Auto-pause for cost optimization
  auto_pause {
    delay_in_minutes = var.enable_cost_optimization ? 15 : -1
  }

  # Spark configuration
  spark_config {
    content  = "spark.sql.adaptive.enabled=true\nspark.sql.adaptive.coalescePartitions.enabled=true"
    filename = "spark-config.conf"
  }

  # Library requirements
  library_requirement {
    content  = file("${path.module}/requirements.txt")
    filename = "requirements.txt"
  }

  tags = var.common_tags
}

# ============================================================================
# POSTGRESQL FLEXIBLE SERVER
# ============================================================================

resource "azurerm_postgresql_flexible_server" "main" {
  name                = "${var.resource_prefix}-postgresql"
  resource_group_name = azurerm_resource_group.main.name
  location           = azurerm_resource_group.main.location

  delegated_subnet_id = azurerm_subnet.data[0].id
  private_dns_zone_id = azurerm_private_dns_zone.postgresql.id

  administrator_login    = "psqladminuser"
  administrator_password = var.postgresql_password

  sku_name   = local.postgresql_skus[var.env_config.cluster_size]
  version    = "15"
  storage_mb = local.postgresql_storage[var.env_config.cluster_size]

  # Backup configuration
  backup_retention_days        = var.env_config.backup_retention
  geo_redundant_backup_enabled = var.env_config.high_availability

  # High availability
  high_availability {
    mode = var.env_config.high_availability ? "ZoneRedundant" : "Disabled"
  }

  # Maintenance window
  maintenance_window {
    day_of_week  = 0  # Sunday
    start_hour   = 3
    start_minute = 0
  }

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgresql]

  tags = var.common_tags
}

# Private DNS zone for PostgreSQL
resource "azurerm_private_dns_zone" "postgresql" {
  name                = "${var.resource_prefix}.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.main.name

  tags = var.common_tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgresql" {
  name                  = "${var.resource_prefix}-postgresql-dns-link"
  private_dns_zone_name = azurerm_private_dns_zone.postgresql.name
  virtual_network_id    = azurerm_virtual_network.main.id
  resource_group_name   = azurerm_resource_group.main.name
  registration_enabled  = false

  tags = var.common_tags
}

# ============================================================================
# LOG ANALYTICS WORKSPACE
# ============================================================================

resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.resource_prefix}-logs"
  location           = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                = "PerGB2018"
  retention_in_days   = var.env_config.backup_retention

  tags = var.common_tags
}

# Application Insights for application monitoring
resource "azurerm_application_insights" "main" {
  name                = "${var.resource_prefix}-appinsights"
  location           = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  workspace_id       = azurerm_log_analytics_workspace.main.id
  application_type   = "web"

  tags = var.common_tags
}

# ============================================================================
# LOCAL VALUES AND VARIABLES
# ============================================================================

locals {
  system_vm_sizes = {
    small  = "Standard_B2s"
    medium = "Standard_D2s_v3"
    large  = "Standard_D4s_v3"
  }

  user_vm_sizes = {
    small  = "Standard_D2s_v3"
    medium = "Standard_D4s_v3"
    large  = "Standard_D8s_v3"
  }

  max_node_counts = {
    small  = 5
    medium = 10
    large  = 20
  }

  user_node_counts = {
    small  = { min = 1, desired = 2, max = 5 }
    medium = { min = 2, desired = 3, max = 8 }
    large  = { min = 3, desired = 5, max = 15 }
  }

  eventhub_partitions = {
    small  = 2
    medium = 4
    large  = 8
  }

  spark_max_nodes = {
    small  = 5
    medium = 10
    large  = 20
  }

  postgresql_skus = {
    small  = "B_Standard_B1ms"
    medium = "GP_Standard_D2s_v3"
    large  = "GP_Standard_D4s_v3"
  }

  postgresql_storage = {
    small  = 32768   # 32 GB
    medium = 131072  # 128 GB
    large  = 524288  # 512 GB
  }
}