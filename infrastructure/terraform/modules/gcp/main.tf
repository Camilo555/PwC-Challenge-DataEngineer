# GCP Cloud Infrastructure Module
# Enterprise-grade GCP resources for data platform

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

# ============================================================================
# NETWORKING AND VPC
# ============================================================================

resource "google_compute_network" "main" {
  name                    = "${var.resource_prefix}-vpc"
  auto_create_subnetworks = false
  mtu                     = 1460

  description = "Main VPC network for ${var.resource_prefix} data platform"
}

# Public subnet for load balancers and NAT gateways
resource "google_compute_subnetwork" "public" {
  count = length(var.availability_zones)

  name          = "${var.resource_prefix}-public-${count.index + 1}"
  ip_cidr_range = cidrsubnet(var.vpc_cidr, 8, count.index)
  region        = var.region
  network       = google_compute_network.main.id

  # Secondary IP ranges for services
  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = cidrsubnet(var.vpc_cidr, 8, count.index + 50)
  }

  # Enable private Google access
  private_ip_google_access = true
}

# Private subnet for GKE nodes and workloads
resource "google_compute_subnetwork" "private" {
  count = length(var.availability_zones)

  name          = "${var.resource_prefix}-private-${count.index + 1}"
  ip_cidr_range = cidrsubnet(var.vpc_cidr, 8, count.index + 10)
  region        = var.region
  network       = google_compute_network.main.id

  # Secondary IP ranges for GKE pods and services
  secondary_ip_range {
    range_name    = "gke-pods"
    ip_cidr_range = cidrsubnet(var.vpc_cidr, 6, count.index + 1)
  }

  secondary_ip_range {
    range_name    = "gke-services"
    ip_cidr_range = cidrsubnet(var.vpc_cidr, 8, count.index + 60)
  }

  # Enable private Google access for APIs
  private_ip_google_access = true
  
  # Log configuration
  log_config {
    aggregation_interval = "INTERVAL_10_MIN"
    flow_sampling       = 0.5
    metadata            = "INCLUDE_ALL_METADATA"
  }
}

# Data subnet for databases and data services
resource "google_compute_subnetwork" "data" {
  count = length(var.availability_zones)

  name          = "${var.resource_prefix}-data-${count.index + 1}"
  ip_cidr_range = cidrsubnet(var.vpc_cidr, 8, count.index + 20)
  region        = var.region
  network       = google_compute_network.main.id

  private_ip_google_access = true
}

# ============================================================================
# FIREWALL RULES
# ============================================================================

# Allow internal communication
resource "google_compute_firewall" "allow_internal" {
  name    = "${var.resource_prefix}-allow-internal"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = [var.vpc_cidr]
  description   = "Allow internal communication within VPC"
}

# Allow HTTP/HTTPS from load balancers
resource "google_compute_firewall" "allow_http_https" {
  name    = "${var.resource_prefix}-allow-http-https"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]  # GCP Load Balancer ranges
  target_tags   = ["http-server", "https-server"]
  description   = "Allow HTTP/HTTPS from Google Load Balancers"
}

# Allow SSH for debugging (restricted)
resource "google_compute_firewall" "allow_ssh" {
  name    = "${var.resource_prefix}-allow-ssh"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = var.ssh_source_ranges
  target_tags   = ["ssh-server"]
  description   = "Allow SSH access from specified ranges"
}

# Health check firewall rule for load balancers
resource "google_compute_firewall" "allow_health_check" {
  name    = "${var.resource_prefix}-allow-health-check"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8080", "80", "443"]
  }

  source_ranges = [
    "130.211.0.0/22",
    "35.191.0.0/16",
    "209.85.152.0/22",
    "209.85.204.0/22"
  ]
  target_tags = ["load-balanced"]
  description = "Allow health checks from GCP load balancers"
}

# ============================================================================
# CLOUD NAT FOR PRIVATE SUBNET INTERNET ACCESS
# ============================================================================

resource "google_compute_router" "nat_router" {
  count = var.env_config.high_availability ? length(var.availability_zones) : 1

  name    = "${var.resource_prefix}-nat-router-${count.index + 1}"
  region  = var.region
  network = google_compute_network.main.id

  bgp {
    asn = 64514
  }
}

resource "google_compute_router_nat" "nat" {
  count = var.env_config.high_availability ? length(var.availability_zones) : 1

  name                               = "${var.resource_prefix}-nat-${count.index + 1}"
  router                            = google_compute_router.nat_router[count.index].name
  region                            = var.region
  nat_ip_allocate_option            = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  # Log configuration for monitoring
  log_config {
    enable = var.env_config.monitoring_level == "comprehensive"
    filter = "ERRORS_ONLY"
  }
}

# ============================================================================
# GKE CLUSTER
# ============================================================================

resource "google_service_account" "gke_nodes" {
  account_id   = "${var.resource_prefix}-gke-nodes"
  display_name = "GKE Node Service Account"
  description  = "Service account for GKE nodes with minimal required permissions"
}

# IAM bindings for GKE node service account
resource "google_project_iam_member" "gke_node_service_account" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/monitoring.viewer",
    "roles/stackdriver.resourceMetadata.writer",
    "roles/storage.objectViewer"
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

# Additional permissions for data access
resource "google_project_iam_member" "gke_data_access" {
  for_each = var.enable_data_access ? toset([
    "roles/storage.admin",
    "roles/bigquery.dataEditor",
    "roles/pubsub.editor"
  ]) : []

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.gke_nodes.email}"
}

resource "google_container_cluster" "main" {
  name     = "${var.resource_prefix}-gke"
  location = var.env_config.high_availability ? var.region : "${var.region}-a"

  # Networking configuration
  network    = google_compute_network.main.id
  subnetwork = google_compute_subnetwork.private[0].id

  # IP allocation for pods and services
  ip_allocation_policy {
    cluster_secondary_range_name  = google_compute_subnetwork.private[0].secondary_ip_range[0].range_name
    services_secondary_range_name = google_compute_subnetwork.private[0].secondary_ip_range[1].range_name
  }

  # Private cluster configuration
  private_cluster_config {
    enable_private_nodes    = true
    enable_private_endpoint = var.environment == "production"
    master_ipv4_cidr_block = "172.16.0.0/28"

    master_global_access_config {
      enabled = var.environment != "production"
    }
  }

  # Master authorized networks for API access
  dynamic "master_authorized_networks_config" {
    for_each = var.environment == "production" ? [1] : []
    content {
      dynamic "cidr_blocks" {
        for_each = var.authorized_networks
        content {
          cidr_block   = cidr_blocks.value.cidr_block
          display_name = cidr_blocks.value.display_name
        }
      }
    }
  }

  # Enable Autopilot for simplified management
  enable_autopilot = var.enable_gke_autopilot

  # Workload Identity for secure pod-to-GCP service communication
  workload_identity_config {
    workload_pool = "${var.project_id}.svc.id.goog"
  }

  # Network policy for micro-segmentation
  network_policy {
    enabled  = true
    provider = "CALICO"
  }

  # Security configurations
  master_auth {
    client_certificate_config {
      issue_client_certificate = false
    }
  }

  # Addons configuration
  addons_config {
    http_load_balancing {
      disabled = false
    }

    horizontal_pod_autoscaling {
      disabled = false
    }

    network_policy_config {
      disabled = false
    }

    dns_cache_config {
      enabled = true
    }

    gcp_filestore_csi_driver_config {
      enabled = var.env_config.cluster_size == "large"
    }

    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  # Binary authorization for image security
  binary_authorization {
    evaluation_mode = var.environment == "production" ? "PROJECT_SINGLETON_POLICY_ENFORCE" : "DISABLED"
  }

  # Database encryption
  database_encryption {
    state    = "ENCRYPTED"
    key_name = google_kms_crypto_key.gke.id
  }

  # Monitoring configuration
  monitoring_config {
    enable_components = var.env_config.monitoring_level == "comprehensive" ? [
      "SYSTEM_COMPONENTS",
      "WORKLOADS",
      "APISERVER",
      "CONTROLLER_MANAGER",
      "SCHEDULER"
    ] : ["SYSTEM_COMPONENTS"]

    managed_prometheus {
      enabled = var.env_config.monitoring_level != "basic"
    }
  }

  logging_config {
    enable_components = var.env_config.monitoring_level == "comprehensive" ? [
      "SYSTEM_COMPONENTS",
      "WORKLOADS",
      "APISERVER",
      "CONTROLLER_MANAGER",
      "SCHEDULER"
    ] : ["SYSTEM_COMPONENTS"]
  }

  # Initial node count - will be managed by node pools
  initial_node_count       = 1
  remove_default_node_pool = true

  # Lifecycle management
  lifecycle {
    ignore_changes = [initial_node_count]
  }

  depends_on = [
    google_project_iam_member.gke_node_service_account,
    google_kms_crypto_key_iam_member.gke_crypto_key
  ]
}

# System node pool for cluster management
resource "google_container_node_pool" "system" {
  count = var.enable_gke_autopilot ? 0 : 1

  name       = "system-pool"
  cluster    = google_container_cluster.main.id
  location   = google_container_cluster.main.location

  initial_node_count = var.env_config.high_availability ? 1 : 1

  autoscaling {
    min_node_count = 1
    max_node_count = var.env_config.high_availability ? 3 : 2
  }

  node_config {
    machine_type = local.system_machine_types[var.env_config.cluster_size]
    disk_type    = "pd-ssd"
    disk_size_gb = 50

    service_account = google_service_account.gke_nodes.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    # Security configurations
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    labels = {
      node-pool = "system"
      workload  = "system"
    }

    tags = ["gke-node", "system-pool"]

    # Preemptible instances for cost optimization
    preemptible = var.enable_cost_optimization && var.env_config.cluster_size == "small"
    spot        = var.enable_cost_optimization && var.env_config.cluster_size != "small"
  }

  upgrade_settings {
    max_surge       = var.env_config.high_availability ? 2 : 1
    max_unavailable = 0
  }

  management {
    auto_repair  = true
    auto_upgrade = var.environment != "production"
  }
}

# User node pool for data processing workloads
resource "google_container_node_pool" "user" {
  count = var.enable_gke_autopilot ? 0 : 1

  name       = "user-pool"
  cluster    = google_container_cluster.main.id
  location   = google_container_cluster.main.location

  initial_node_count = local.user_node_counts[var.env_config.cluster_size].desired

  autoscaling {
    min_node_count = local.user_node_counts[var.env_config.cluster_size].min
    max_node_count = local.user_node_counts[var.env_config.cluster_size].max
  }

  node_config {
    machine_type = local.user_machine_types[var.env_config.cluster_size]
    disk_type    = "pd-ssd"
    disk_size_gb = var.env_config.cluster_size == "large" ? 200 : 100

    service_account = google_service_account.gke_nodes.email
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]

    # Security configurations
    shielded_instance_config {
      enable_secure_boot          = true
      enable_integrity_monitoring = true
    }

    workload_metadata_config {
      mode = "GKE_METADATA"
    }

    # Resource-optimized configurations
    guest_accelerator {
      count = var.env_config.cluster_size == "large" && var.enable_gpu_acceleration ? 1 : 0
      type  = "nvidia-tesla-k80"
    }

    labels = {
      node-pool = "user"
      workload  = "data-processing"
    }

    tags = ["gke-node", "user-pool", "data-processing"]

    # Cost optimization
    preemptible = var.enable_cost_optimization && var.env_config.cluster_size == "small"
    spot        = var.enable_cost_optimization && var.env_config.cluster_size != "small"

    # Node taints for dedicated workloads
    taint {
      key    = "workload"
      value  = "data-processing"
      effect = "NO_SCHEDULE"
    }
  }

  upgrade_settings {
    max_surge       = var.env_config.high_availability ? 2 : 1
    max_unavailable = 0
  }

  management {
    auto_repair  = true
    auto_upgrade = var.environment != "production"
  }
}

# ============================================================================
# DATA STORAGE (CLOUD STORAGE)
# ============================================================================

resource "google_storage_bucket" "data_lake" {
  name          = "${var.resource_prefix}-data-lake-${random_string.bucket_suffix.result}"
  location      = var.env_config.high_availability ? var.region : "${var.region}-a"
  storage_class = "STANDARD"
  
  # Uniform bucket-level access
  uniform_bucket_level_access = true

  # Versioning for data lineage
  versioning {
    enabled = true
  }

  # Encryption with customer-managed key
  encryption {
    default_kms_key_name = google_kms_crypto_key.storage.id
  }

  # Lifecycle management for cost optimization
  dynamic "lifecycle_rule" {
    for_each = var.enable_cost_optimization ? [1] : []
    content {
      condition {
        age = 30
      }
      action {
        type          = "SetStorageClass"
        storage_class = "NEARLINE"
      }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.enable_cost_optimization ? [1] : []
    content {
      condition {
        age = 90
      }
      action {
        type          = "SetStorageClass"
        storage_class = "COLDLINE"
      }
    }
  }

  dynamic "lifecycle_rule" {
    for_each = var.enable_cost_optimization ? [1] : []
    content {
      condition {
        age = 365
      }
      action {
        type          = "SetStorageClass"
        storage_class = "ARCHIVE"
      }
    }
  }

  # Retention policy
  retention_policy {
    retention_period = var.env_config.backup_retention * 24 * 3600  # Convert days to seconds
  }

  # Logging
  logging {
    log_bucket        = google_storage_bucket.logs.name
    log_object_prefix = "data-lake-access-logs/"
  }

  labels = {
    environment = var.environment
    purpose     = "data-lake"
    tier        = "bronze"
  }
}

# Separate buckets for different data tiers
resource "google_storage_bucket" "silver_data" {
  name          = "${var.resource_prefix}-silver-data-${random_string.bucket_suffix.result}"
  location      = var.env_config.high_availability ? var.region : "${var.region}-a"
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }

  encryption {
    default_kms_key_name = google_kms_crypto_key.storage.id
  }

  labels = {
    environment = var.environment
    purpose     = "data-lake"
    tier        = "silver"
  }
}

resource "google_storage_bucket" "gold_data" {
  name          = "${var.resource_prefix}-gold-data-${random_string.bucket_suffix.result}"
  location      = var.env_config.high_availability ? var.region : "${var.region}-a"
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true
  versioning {
    enabled = true
  }

  encryption {
    default_kms_key_name = google_kms_crypto_key.storage.id
  }

  labels = {
    environment = var.environment
    purpose     = "data-lake"
    tier        = "gold"
  }
}

# Logs bucket
resource "google_storage_bucket" "logs" {
  name          = "${var.resource_prefix}-logs-${random_string.bucket_suffix.result}"
  location      = var.env_config.high_availability ? var.region : "${var.region}-a"
  storage_class = "STANDARD"
  
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = var.env_config.backup_retention
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    environment = var.environment
    purpose     = "logging"
  }
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

# ============================================================================
# BIGQUERY DATA WAREHOUSE
# ============================================================================

resource "google_bigquery_dataset" "main" {
  dataset_id  = "${replace(var.resource_prefix, "-", "_")}_datawarehouse"
  location    = var.region
  description = "Main data warehouse dataset for ${var.resource_prefix}"

  # Access control
  access {
    role          = "OWNER"
    user_by_email = var.bigquery_admin_email
  }

  access {
    role           = "READER"
    special_group  = "projectReaders"
  }

  access {
    role           = "WRITER"
    special_group  = "projectWriters"
  }

  # Default encryption
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery.id
  }

  # Default table expiration (optional)
  default_table_expiration_ms = var.enable_cost_optimization ? (var.env_config.backup_retention * 24 * 3600 * 1000) : null

  labels = {
    environment = var.environment
    purpose     = "datawarehouse"
  }
}

# External tables for data lake integration
resource "google_bigquery_table" "bronze_external" {
  dataset_id = google_bigquery_dataset.main.dataset_id
  table_id   = "bronze_data_external"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${google_storage_bucket.data_lake.name}/bronze/*"]
  }

  labels = {
    tier        = "bronze"
    type        = "external"
    environment = var.environment
  }
}

# ============================================================================
# PUB/SUB FOR STREAMING DATA
# ============================================================================

resource "google_pubsub_topic" "data_stream" {
  name = "${var.resource_prefix}-data-stream"

  # Message retention
  message_retention_duration = "${var.env_config.backup_retention * 24}h"

  # Encryption
  kms_key_name = google_kms_crypto_key.pubsub.id

  labels = {
    environment = var.environment
    purpose     = "streaming-data"
  }
}

resource "google_pubsub_subscription" "data_processing" {
  name  = "${var.resource_prefix}-data-processing"
  topic = google_pubsub_topic.data_stream.name

  # Acknowledgment deadline
  ack_deadline_seconds = 600

  # Message retention
  message_retention_duration = "7200s"  # 2 hours

  # Dead letter queue
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Push to Cloud Run or pull
  push_config {
    push_endpoint = var.pubsub_push_endpoint != "" ? var.pubsub_push_endpoint : null
  }

  labels = {
    environment = var.environment
    consumer    = "data-processing"
  }
}

resource "google_pubsub_topic" "dead_letter" {
  name = "${var.resource_prefix}-dead-letter"

  labels = {
    environment = var.environment
    purpose     = "dead-letter"
  }
}

# ============================================================================
# CLOUD SQL (POSTGRESQL)
# ============================================================================

resource "google_sql_database_instance" "main" {
  count = var.enable_cloud_sql ? 1 : 0

  name             = "${var.resource_prefix}-postgresql"
  database_version = "POSTGRES_15"
  region          = var.region
  deletion_protection = var.environment == "production"

  settings {
    tier = local.cloud_sql_tiers[var.env_config.cluster_size]
    
    # Availability and durability
    availability_type = var.env_config.high_availability ? "REGIONAL" : "ZONAL"
    
    backup_configuration {
      enabled                        = true
      start_time                    = "03:00"
      point_in_time_recovery_enabled = true
      backup_retention_settings {
        retained_backups = var.env_config.backup_retention
      }
      
      location = var.region
      
      transaction_log_retention_days = 7
    }

    # IP configuration
    ip_configuration {
      ipv4_enabled                                  = false
      private_network                              = google_compute_network.main.id
      enable_private_path_for_google_cloud_services = true
      
      authorized_networks {
        name  = "vpc-access"
        value = var.vpc_cidr
      }
    }

    # Database flags for optimization
    database_flags {
      name  = "shared_preload_libraries"
      value = "pg_stat_statements,pg_cron"
    }

    database_flags {
      name  = "max_connections"
      value = local.max_connections[var.env_config.cluster_size]
    }

    database_flags {
      name  = "work_mem"
      value = local.work_mem[var.env_config.cluster_size]
    }

    # Disk configuration
    disk_type       = "PD_SSD"
    disk_size       = local.disk_sizes[var.env_config.cluster_size]
    disk_autoresize = true

    # User labels
    user_labels = {
      environment = var.environment
      purpose     = "operational-database"
    }

    # Maintenance window
    maintenance_window {
      day          = 7  # Sunday
      hour         = 3  # 3 AM
      update_track = "stable"
    }

    # Insights configuration
    insights_config {
      query_insights_enabled  = var.env_config.monitoring_level != "basic"
      query_string_length     = 1024
      record_application_tags = true
      record_client_address   = true
    }
  }

  depends_on = [google_service_networking_connection.private_vpc_connection]
}

# Database user
resource "google_sql_user" "main" {
  count = var.enable_cloud_sql ? 1 : 0

  name     = "dataplatform"
  instance = google_sql_database_instance.main[0].name
  password = var.cloud_sql_password
  type     = "BUILT_IN"
}

# Application database
resource "google_sql_database" "main" {
  count = var.enable_cloud_sql ? 1 : 0

  name     = "dataplatform"
  instance = google_sql_database_instance.main[0].name
}

# Private service connection for Cloud SQL
resource "google_service_networking_connection" "private_vpc_connection" {
  count = var.enable_cloud_sql ? 1 : 0

  network                 = google_compute_network.main.id
  service                = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address[0].name]
}

resource "google_compute_global_address" "private_ip_address" {
  count = var.enable_cloud_sql ? 1 : 0

  name          = "${var.resource_prefix}-private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
}

# ============================================================================
# KMS ENCRYPTION KEYS
# ============================================================================

resource "google_kms_key_ring" "main" {
  name     = "${var.resource_prefix}-keyring"
  location = var.region
}

# GKE cluster encryption key
resource "google_kms_crypto_key" "gke" {
  name     = "gke-key"
  key_ring = google_kms_key_ring.main.id
  purpose  = "ENCRYPT_DECRYPT"

  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }

  rotation_period = "2592000s"  # 30 days
}

# Storage encryption key
resource "google_kms_crypto_key" "storage" {
  name     = "storage-key"
  key_ring = google_kms_key_ring.main.id
  purpose  = "ENCRYPT_DECRYPT"

  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }

  rotation_period = "2592000s"  # 30 days
}

# BigQuery encryption key
resource "google_kms_crypto_key" "bigquery" {
  name     = "bigquery-key"
  key_ring = google_kms_key_ring.main.id
  purpose  = "ENCRYPT_DECRYPT"

  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }

  rotation_period = "2592000s"  # 30 days
}

# Pub/Sub encryption key
resource "google_kms_crypto_key" "pubsub" {
  name     = "pubsub-key"
  key_ring = google_kms_key_ring.main.id
  purpose  = "ENCRYPT_DECRYPT"

  version_template {
    algorithm = "GOOGLE_SYMMETRIC_ENCRYPTION"
  }

  rotation_period = "2592000s"  # 30 days
}

# IAM binding for GKE to use the encryption key
resource "google_kms_crypto_key_iam_member" "gke_crypto_key" {
  crypto_key_id = google_kms_crypto_key.gke.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  
  member = "serviceAccount:service-${data.google_project.project.number}@container-engine-robot.iam.gserviceaccount.com"
}

# ============================================================================
# LOCALS AND DATA SOURCES
# ============================================================================

data "google_project" "project" {
  project_id = var.project_id
}

locals {
  system_machine_types = {
    small  = "e2-standard-2"
    medium = "e2-standard-4"
    large  = "e2-standard-8"
  }

  user_machine_types = {
    small  = "n1-standard-4"
    medium = "n1-standard-8"
    large  = "n1-standard-16"
  }

  user_node_counts = {
    small  = { min = 1, desired = 2, max = 5 }
    medium = { min = 2, desired = 3, max = 10 }
    large  = { min = 3, desired = 5, max = 20 }
  }

  cloud_sql_tiers = {
    small  = "db-custom-2-4096"   # 2 vCPUs, 4 GB RAM
    medium = "db-custom-4-8192"   # 4 vCPUs, 8 GB RAM
    large  = "db-custom-8-16384"  # 8 vCPUs, 16 GB RAM
  }

  disk_sizes = {
    small  = 50
    medium = 100
    large  = 200
  }

  max_connections = {
    small  = "100"
    medium = "200"
    large  = "500"
  }

  work_mem = {
    small  = "4MB"
    medium = "8MB"
    large  = "16MB"
  }
}