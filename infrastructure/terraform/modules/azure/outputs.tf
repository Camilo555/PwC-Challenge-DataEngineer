# Azure Infrastructure Module Outputs
# Output values for integration with other modules

# ============================================================================
# RESOURCE GROUP INFORMATION
# ============================================================================

output "resource_group_name" {
  description = "Name of the Azure resource group"
  value       = azurerm_resource_group.main.name
}

output "resource_group_id" {
  description = "ID of the Azure resource group"
  value       = azurerm_resource_group.main.id
}

output "location" {
  description = "Azure location/region"
  value       = azurerm_resource_group.main.location
}

# ============================================================================
# NETWORKING INFORMATION
# ============================================================================

output "vnet_id" {
  description = "ID of the virtual network"
  value       = azurerm_virtual_network.main.id
}

output "vnet_name" {
  description = "Name of the virtual network"
  value       = azurerm_virtual_network.main.name
}

output "public_subnet_ids" {
  description = "IDs of public subnets"
  value       = azurerm_subnet.public[*].id
}

output "private_subnet_ids" {
  description = "IDs of private subnets"
  value       = azurerm_subnet.private[*].id
}

output "data_subnet_ids" {
  description = "IDs of data subnets"
  value       = azurerm_subnet.data[*].id
}

output "gateway_subnet_id" {
  description = "ID of the gateway subnet"
  value       = azurerm_subnet.gateway.id
}

output "network_security_groups" {
  description = "Network security group information"
  value = {
    aks_nodes      = azurerm_network_security_group.aks_nodes.id
    data_services  = azurerm_network_security_group.data_services.id
  }
}

# ============================================================================
# AKS CLUSTER INFORMATION
# ============================================================================

output "aks_cluster_id" {
  description = "ID of the AKS cluster"
  value       = azurerm_kubernetes_cluster.main.id
}

output "aks_cluster_name" {
  description = "Name of the AKS cluster"
  value       = azurerm_kubernetes_cluster.main.name
}

output "aks_cluster_endpoint" {
  description = "AKS cluster API server endpoint"
  value       = azurerm_kubernetes_cluster.main.kube_config.0.host
  sensitive   = true
}

output "aks_cluster_ca" {
  description = "AKS cluster certificate authority data"
  value       = azurerm_kubernetes_cluster.main.kube_config.0.cluster_ca_certificate
  sensitive   = true
}

output "aks_cluster_fqdn" {
  description = "AKS cluster FQDN"
  value       = azurerm_kubernetes_cluster.main.fqdn
}

output "aks_cluster_identity" {
  description = "AKS cluster managed identity"
  value = {
    principal_id = azurerm_kubernetes_cluster.main.identity[0].principal_id
    tenant_id    = azurerm_kubernetes_cluster.main.identity[0].tenant_id
  }
}

output "aks_node_resource_group" {
  description = "AKS node resource group name"
  value       = azurerm_kubernetes_cluster.main.node_resource_group
}

# ============================================================================
# KEY VAULT INFORMATION
# ============================================================================

output "key_vault_id" {
  description = "ID of the Key Vault"
  value       = azurerm_key_vault.main.id
}

output "key_vault_name" {
  description = "Name of the Key Vault"
  value       = azurerm_key_vault.main.name
}

output "key_vault_uri" {
  description = "URI of the Key Vault"
  value       = azurerm_key_vault.main.vault_uri
}

# ============================================================================
# DATA LAKE STORAGE INFORMATION
# ============================================================================

output "data_lake_storage" {
  description = "Data Lake Storage account information"
  value = {
    name                = azurerm_storage_account.data_lake.name
    id                  = azurerm_storage_account.data_lake.id
    primary_endpoint    = azurerm_storage_account.data_lake.primary_dfs_endpoint
    primary_access_key  = azurerm_storage_account.data_lake.primary_access_key
    connection_string   = azurerm_storage_account.data_lake.primary_connection_string
  }
  sensitive = true
}

output "data_lake_filesystems" {
  description = "Data Lake Gen2 filesystem information"
  value = {
    bronze = azurerm_storage_data_lake_gen2_filesystem.bronze.name
    silver = azurerm_storage_data_lake_gen2_filesystem.silver.name
    gold   = azurerm_storage_data_lake_gen2_filesystem.gold.name
  }
}

# ============================================================================
# EVENT HUBS INFORMATION
# ============================================================================

output "event_hubs" {
  description = "Event Hubs namespace and hub information"
  value = {
    namespace_name      = azurerm_eventhub_namespace.main.name
    namespace_id        = azurerm_eventhub_namespace.main.id
    connection_string   = azurerm_eventhub_namespace.main.default_primary_connection_string
    data_stream_name    = azurerm_eventhub.data_stream.name
    consumer_groups     = {
      data_processing      = azurerm_eventhub_consumer_group.data_processing.name
      real_time_analytics  = azurerm_eventhub_consumer_group.real_time_analytics.name
    }
  }
  sensitive = true
}

# ============================================================================
# SYNAPSE ANALYTICS INFORMATION
# ============================================================================

output "synapse_workspace" {
  description = "Synapse Analytics workspace information"
  value = var.env_config.cluster_size != "small" ? {
    id                    = azurerm_synapse_workspace.main[0].id
    name                  = azurerm_synapse_workspace.main[0].name
    connectivity_endpoints = azurerm_synapse_workspace.main[0].connectivity_endpoints
    sql_administrator_login = azurerm_synapse_workspace.main[0].sql_administrator_login
    identity              = azurerm_synapse_workspace.main[0].identity
  } : null
  sensitive = true
}

output "synapse_sql_pool" {
  description = "Synapse SQL Pool information"
  value = var.env_config.cluster_size == "large" ? {
    name = azurerm_synapse_sql_pool.main[0].name
    id   = azurerm_synapse_sql_pool.main[0].id
  } : null
}

output "synapse_spark_pool" {
  description = "Synapse Spark Pool information"
  value = var.env_config.cluster_size != "small" ? {
    name = azurerm_synapse_spark_pool.main[0].name
    id   = azurerm_synapse_spark_pool.main[0].id
  } : null
}

# ============================================================================
# POSTGRESQL INFORMATION
# ============================================================================

output "postgresql_server" {
  description = "PostgreSQL Flexible Server information"
  value = {
    name                = azurerm_postgresql_flexible_server.main.name
    id                  = azurerm_postgresql_flexible_server.main.id
    fqdn               = azurerm_postgresql_flexible_server.main.fqdn
    administrator_login = azurerm_postgresql_flexible_server.main.administrator_login
  }
  sensitive = true
}

# ============================================================================
# MONITORING INFORMATION
# ============================================================================

output "log_analytics_workspace" {
  description = "Log Analytics workspace information"
  value = {
    id           = azurerm_log_analytics_workspace.main.id
    workspace_id = azurerm_log_analytics_workspace.main.workspace_id
    name         = azurerm_log_analytics_workspace.main.name
    primary_shared_key = azurerm_log_analytics_workspace.main.primary_shared_key
  }
  sensitive = true
}

output "log_analytics_workspace_resource_id" {
  description = "Log Analytics workspace resource ID for cross-cloud networking"
  value = azurerm_log_analytics_workspace.main.id
}

output "application_insights" {
  description = "Application Insights information"
  value = {
    id                  = azurerm_application_insights.main.id
    name                = azurerm_application_insights.main.name
    instrumentation_key = azurerm_application_insights.main.instrumentation_key
    connection_string   = azurerm_application_insights.main.connection_string
  }
  sensitive = true
}

# ============================================================================
# COST OPTIMIZATION INFORMATION
# ============================================================================

output "cost_optimization_features" {
  description = "Status of cost optimization features"
  value = {
    spot_instances_enabled    = var.enable_cost_optimization
    auto_scaling_enabled     = var.env_config.auto_scaling
    lifecycle_policies_enabled = var.enable_cost_optimization
    auto_pause_synapse       = var.enable_cost_optimization
    storage_tiering_enabled  = var.enable_cost_optimization
  }
}

# ============================================================================
# DISASTER RECOVERY INFORMATION
# ============================================================================

output "disaster_recovery_configuration" {
  description = "Disaster recovery configuration status"
  value = {
    geo_redundant_storage    = var.env_config.high_availability
    multi_zone_deployment    = var.env_config.high_availability
    backup_retention_days    = var.env_config.backup_retention
    high_availability_enabled = var.env_config.high_availability
  }
}

# ============================================================================
# COMPREHENSIVE AZURE SUMMARY
# ============================================================================

output "azure_infrastructure_summary" {
  description = "Comprehensive summary of Azure infrastructure components"
  value = {
    # Core Infrastructure
    resource_group = azurerm_resource_group.main.name
    location       = azurerm_resource_group.main.location
    vnet_name      = azurerm_virtual_network.main.name
    
    # Compute
    aks_cluster = {
      name              = azurerm_kubernetes_cluster.main.name
      kubernetes_version = azurerm_kubernetes_cluster.main.kubernetes_version
      node_count        = azurerm_kubernetes_cluster.main.default_node_pool[0].node_count
      vm_size           = azurerm_kubernetes_cluster.main.default_node_pool[0].vm_size
    }
    
    # Data Services
    data_lake_name = azurerm_storage_account.data_lake.name
    eventhub_namespace = azurerm_eventhub_namespace.main.name
    postgresql_server = azurerm_postgresql_flexible_server.main.name
    synapse_enabled = var.env_config.cluster_size != "small"
    
    # Security
    key_vault_name = azurerm_key_vault.main.name
    private_endpoints_enabled = var.environment == "production"
    network_security_groups = length(azurerm_network_security_group.aks_nodes) + length(azurerm_network_security_group.data_services)
    
    # Monitoring
    log_analytics_workspace = azurerm_log_analytics_workspace.main.name
    application_insights = azurerm_application_insights.main.name
    
    # High Availability & DR
    high_availability = var.env_config.high_availability
    availability_zones = var.availability_zones
    backup_retention = var.env_config.backup_retention
    
    # Cost Optimization
    cost_optimization_enabled = var.enable_cost_optimization
    auto_scaling_enabled = var.env_config.auto_scaling
    spot_instances_used = var.enable_cost_optimization
  }
}

# ============================================================================
# INTEGRATION ENDPOINTS
# ============================================================================

output "integration_endpoints" {
  description = "Key endpoints for integration with other cloud providers and services"
  value = {
    # Network integration
    vnet_address_space = azurerm_virtual_network.main.address_space
    gateway_subnet_id  = azurerm_subnet.gateway.id
    
    # Data integration
    data_lake_endpoint = azurerm_storage_account.data_lake.primary_dfs_endpoint
    eventhub_connection_string = azurerm_eventhub_namespace.main.default_primary_connection_string
    
    # Compute integration
    aks_api_endpoint = azurerm_kubernetes_cluster.main.kube_config.0.host
    
    # Monitoring integration
    log_analytics_workspace_id = azurerm_log_analytics_workspace.main.workspace_id
  }
  sensitive = true
}