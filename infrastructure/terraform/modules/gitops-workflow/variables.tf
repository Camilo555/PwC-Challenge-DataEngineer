# GitOps Workflow Variables
# Configuration for automated deployment and validation

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "data-platform"
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "environments" {
  description = "List of environments to manage"
  type        = list(string)
  default     = ["development", "staging", "production"]
}

variable "cloud_provider" {
  description = "Primary cloud provider"
  type        = string
  default     = "multi"
}

# GitLab Configuration
variable "gitlab_project_id" {
  description = "GitLab project ID for CI/CD"
  type        = string
}

variable "gitlab_project_path" {
  description = "GitLab project path (namespace/project)"
  type        = string
}

variable "gitlab_project_url" {
  description = "GitLab project URL for ArgoCD"
  type        = string
}

# Terraform Cloud Configuration
variable "terraform_cloud_organization" {
  description = "Terraform Cloud organization name"
  type        = string
}

variable "terraform_cloud_token" {
  description = "Terraform Cloud API token"
  type        = string
  sensitive   = true
}

variable "terraform_version" {
  description = "Terraform version to use"
  type        = string
  default     = "1.5.0"
}

variable "vcs_oauth_token_id" {
  description = "VCS OAuth token ID for Terraform Cloud"
  type        = string
}

# ArgoCD Configuration
variable "argocd_version" {
  description = "ArgoCD Helm chart version"
  type        = string
  default     = "5.46.0"
}

variable "argocd_public_access" {
  description = "Enable public access to ArgoCD server"
  type        = bool
  default     = false
}

variable "argocd_server_url" {
  description = "ArgoCD server URL for API access"
  type        = string
  default     = "http://argocd-server.argocd.svc.cluster.local"
}

variable "enable_sso_integration" {
  description = "Enable SSO integration for ArgoCD"
  type        = bool
  default     = true
}

# AWS Configuration
variable "enable_aws_deployment" {
  description = "Enable AWS deployment capabilities"
  type        = bool
  default     = true
}

variable "aws_access_key_id" {
  description = "AWS Access Key ID"
  type        = string
  sensitive   = true
  default     = ""
}

variable "aws_secret_access_key" {
  description = "AWS Secret Access Key"
  type        = string
  sensitive   = true
  default     = ""
}

# Azure Configuration
variable "enable_azure_deployment" {
  description = "Enable Azure deployment capabilities"
  type        = bool
  default     = true
}

variable "azure_client_id" {
  description = "Azure Client ID"
  type        = string
  sensitive   = true
  default     = ""
}

variable "azure_client_secret" {
  description = "Azure Client Secret"
  type        = string
  sensitive   = true
  default     = ""
}

variable "azure_tenant_id" {
  description = "Azure Tenant ID"
  type        = string
  sensitive   = true
  default     = ""
}

variable "azure_subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
  default     = ""
}

# GCP Configuration
variable "enable_gcp_deployment" {
  description = "Enable GCP deployment capabilities"
  type        = bool
  default     = true
}

variable "gcp_service_account_key" {
  description = "GCP Service Account Key JSON"
  type        = string
  sensitive   = true
  default     = ""
}

# Validation Pipeline Configuration
variable "enable_validation_pipeline" {
  description = "Enable infrastructure validation pipeline"
  type        = bool
  default     = true
}

variable "enable_drift_detection" {
  description = "Enable infrastructure drift detection"
  type        = bool
  default     = true
}

variable "drift_detection_schedule" {
  description = "Cron schedule for drift detection"
  type        = string
  default     = "0 */6 * * *"  # Every 6 hours
}

variable "webhook_url" {
  description = "Webhook URL for alerts"
  type        = string
  default     = ""
}

# Rollback Configuration
variable "enable_rollback_automation" {
  description = "Enable automated rollback on deployment failures"
  type        = bool
  default     = true
}

variable "rollback_timeout_minutes" {
  description = "Timeout for rollback operations in minutes"
  type        = number
  default     = 30
}

# Monitoring Configuration
variable "enable_monitoring" {
  description = "Enable monitoring and metrics collection"
  type        = bool
  default     = true
}

variable "metrics_retention_days" {
  description = "Metrics retention period in days"
  type        = number
  default     = 30
}

# Security Configuration
variable "enable_policy_validation" {
  description = "Enable policy validation with OPA/Conftest"
  type        = bool
  default     = true
}

variable "enable_security_scanning" {
  description = "Enable security scanning with tfsec"
  type        = bool
  default     = true
}

variable "enable_compliance_checks" {
  description = "Enable compliance validation"
  type        = bool
  default     = true
}

# Performance Configuration
variable "parallel_deployments" {
  description = "Number of parallel deployments allowed"
  type        = number
  default     = 3
}

variable "deployment_timeout_minutes" {
  description = "Deployment timeout in minutes"
  type        = number
  default     = 60
}

# Cost Optimization
variable "enable_cost_estimation" {
  description = "Enable cost estimation in CI/CD pipeline"
  type        = bool
  default     = true
}

variable "cost_threshold_dollars" {
  description = "Cost threshold for deployment approval (USD)"
  type        = number
  default     = 1000
}

# Notification Configuration
variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  sensitive   = true
  default     = ""
}

variable "teams_webhook_url" {
  description = "Microsoft Teams webhook URL for notifications"
  type        = string
  sensitive   = true
  default     = ""
}

variable "enable_email_notifications" {
  description = "Enable email notifications"
  type        = bool
  default     = true
}

variable "notification_email_list" {
  description = "List of email addresses for notifications"
  type        = list(string)
  default     = []
}

# Backup and Recovery
variable "enable_state_backup" {
  description = "Enable Terraform state backup"
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "State backup retention period in days"
  type        = number
  default     = 90
}

variable "enable_cross_region_backup" {
  description = "Enable cross-region state backup"
  type        = bool
  default     = true
}

# Testing Configuration
variable "enable_integration_tests" {
  description = "Enable integration testing in pipeline"
  type        = bool
  default     = true
}

variable "enable_smoke_tests" {
  description = "Enable smoke testing after deployment"
  type        = bool
  default     = true
}

variable "test_timeout_minutes" {
  description = "Test execution timeout in minutes"
  type        = number
  default     = 30
}

# Advanced GitOps Features
variable "enable_progressive_delivery" {
  description = "Enable progressive delivery with canary deployments"
  type        = bool
  default     = false
}

variable "canary_deployment_percentage" {
  description = "Percentage of traffic for canary deployments"
  type        = number
  default     = 10
}

variable "enable_blue_green_deployment" {
  description = "Enable blue-green deployment strategy"
  type        = bool
  default     = false
}

variable "health_check_path" {
  description = "Health check path for deployments"
  type        = string
  default     = "/health"
}

# Resource Limits
variable "cpu_limit" {
  description = "CPU limit for GitOps containers"
  type        = string
  default     = "1000m"
}

variable "memory_limit" {
  description = "Memory limit for GitOps containers"
  type        = string
  default     = "1Gi"
}

variable "storage_request" {
  description = "Storage request for persistent volumes"
  type        = string
  default     = "10Gi"
}

# Additional Tags
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}