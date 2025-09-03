# GitOps Workflow Module
# Automated deployment validation and state management
# Features: GitLab CI/CD, ArgoCD, Terraform Cloud integration

terraform {
  required_providers {
    gitlab = {
      source  = "gitlabhq/gitlab"
      version = "~> 16.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
    tfe = {
      source  = "hashicorp/tfe"
      version = "~> 0.47"
    }
  }
  required_version = ">= 1.5"
}

# GitLab CI/CD Pipeline Configuration
resource "gitlab_project_variable" "terraform_cloud_token" {
  project   = var.gitlab_project_id
  key       = "TFC_TEAM_TOKEN"
  value     = var.terraform_cloud_token
  protected = true
  masked    = true
}

resource "gitlab_project_variable" "aws_credentials" {
  count     = var.enable_aws_deployment ? 1 : 0
  project   = var.gitlab_project_id
  key       = "AWS_ACCESS_KEY_ID"
  value     = var.aws_access_key_id
  protected = true
  masked    = true
}

resource "gitlab_project_variable" "aws_secret" {
  count     = var.enable_aws_deployment ? 1 : 0
  project   = var.gitlab_project_id
  key       = "AWS_SECRET_ACCESS_KEY"
  value     = var.aws_secret_access_key
  protected = true
  masked    = true
}

resource "gitlab_project_variable" "azure_credentials" {
  count     = var.enable_azure_deployment ? 1 : 0
  project   = var.gitlab_project_id
  key       = "AZURE_CLIENT_ID"
  value     = var.azure_client_id
  protected = true
  masked    = true
}

resource "gitlab_project_variable" "gcp_credentials" {
  count     = var.enable_gcp_deployment ? 1 : 0
  project   = var.gitlab_project_id
  key       = "GOOGLE_CREDENTIALS"
  value     = var.gcp_service_account_key
  protected = true
  masked    = true
}

# Terraform Cloud Workspace Configuration
resource "tfe_workspace" "infrastructure" {
  count            = length(var.environments)
  name             = "${var.project_name}-${var.environments[count.index]}"
  organization     = var.terraform_cloud_organization
  terraform_version = var.terraform_version
  
  auto_apply = var.environments[count.index] == "production" ? false : true
  
  vcs_repo {
    identifier     = var.gitlab_project_path
    branch         = var.environments[count.index] == "production" ? "main" : var.environments[count.index]
    oauth_token_id = var.vcs_oauth_token_id
  }
  
  working_directory = "infrastructure/terraform"
  
  trigger_prefixes = [
    "infrastructure/terraform",
    "modules/",
    ".terraform-version"
  ]
  
  tag_names = [
    "environment:${var.environments[count.index]}",
    "managed-by:terraform",
    "gitops:enabled"
  ]
}

# Environment-specific workspace variables
resource "tfe_variable" "environment" {
  count        = length(var.environments)
  key          = "environment"
  value        = var.environments[count.index]
  category     = "terraform"
  workspace_id = tfe_workspace.infrastructure[count.index].id
}

resource "tfe_variable" "cloud_provider" {
  count        = length(var.environments)
  key          = "cloud_provider"
  value        = var.cloud_provider
  category     = "terraform"
  workspace_id = tfe_workspace.infrastructure[count.index].id
}

# Sensitive variables for cloud credentials
resource "tfe_variable" "aws_access_key" {
  count        = var.enable_aws_deployment ? length(var.environments) : 0
  key          = "AWS_ACCESS_KEY_ID"
  value        = var.aws_access_key_id
  category     = "env"
  sensitive    = true
  workspace_id = tfe_workspace.infrastructure[count.index].id
}

resource "tfe_variable" "aws_secret_key" {
  count        = var.enable_aws_deployment ? length(var.environments) : 0
  key          = "AWS_SECRET_ACCESS_KEY"
  value        = var.aws_secret_access_key
  category     = "env"
  sensitive    = true
  workspace_id = tfe_workspace.infrastructure[count.index].id
}

# ArgoCD Installation for Kubernetes GitOps
resource "helm_release" "argocd" {
  name       = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argo-cd"
  namespace  = "argocd"
  version    = var.argocd_version
  
  create_namespace = true
  
  values = [yamlencode({
    server = {
      service = {
        type = "LoadBalancer"
        annotations = {
          "service.beta.kubernetes.io/aws-load-balancer-type" = "nlb"
          "service.beta.kubernetes.io/aws-load-balancer-scheme" = var.argocd_public_access ? "internet-facing" : "internal"
        }
      }
      extraArgs = [
        "--insecure"  # Remove in production with proper TLS
      ]
      config = {
        "application.instanceLabelKey" = "argocd.argoproj.io/instance"
        "server.rbac.log.enforce.enable" = "true"
        "exec.enabled" = "false"
      }
      rbacConfig = {
        "g, argocd-admins, role:admin"
      }
    }
    
    controller = {
      metrics = {
        enabled = true
      }
    }
    
    dex = {
      enabled = var.enable_sso_integration
    }
    
    redis = {
      enabled = true
    }
    
    repoServer = {
      metrics = {
        enabled = true
      }
    }
  })]
  
  depends_on = [kubernetes_namespace.argocd]
}

resource "kubernetes_namespace" "argocd" {
  metadata {
    name = "argocd"
    labels = {
      "name" = "argocd"
      "istio-injection" = "disabled"
    }
  }
}

# ArgoCD Application for Infrastructure Management
resource "kubernetes_manifest" "infrastructure_application" {
  count = length(var.environments)
  
  manifest = {
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "${var.project_name}-infrastructure-${var.environments[count.index]}"
      namespace = "argocd"
      labels = {
        environment = var.environments[count.index]
        component   = "infrastructure"
      }
    }
    spec = {
      project = "default"
      source = {
        repoURL        = var.gitlab_project_url
        targetRevision = var.environments[count.index] == "production" ? "main" : var.environments[count.index]
        path           = "kubernetes/manifests/${var.environments[count.index]}"
      }
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "default"
      }
      syncPolicy = {
        automated = {
          prune    = var.environments[count.index] != "production"
          selfHeal = true
        }
        syncOptions = [
          "CreateNamespace=true",
          "ApplyOutOfSyncOnly=true"
        ]
        retry = {
          limit = 5
          backoff = {
            duration    = "5s"
            factor      = 2
            maxDuration = "3m"
          }
        }
      }
    }
  }
  
  depends_on = [helm_release.argocd]
}

# Infrastructure Validation Pipeline
resource "kubernetes_job" "infrastructure_validation" {
  count = var.enable_validation_pipeline ? 1 : 0
  
  metadata {
    name      = "infrastructure-validation"
    namespace = "argocd"
  }
  
  spec {
    template {
      metadata {
        labels = {
          app = "infrastructure-validation"
        }
      }
      
      spec {
        container {
          name  = "terraform-validator"
          image = "hashicorp/terraform:${var.terraform_version}"
          
          command = ["/bin/sh", "-c"]
          args = [
            <<EOF
              echo "Validating Terraform configuration..."
              terraform init -backend=false
              terraform validate
              terraform fmt -check -recursive
              
              echo "Running security scan..."
              curl -sSfL https://github.com/aquasecurity/tfsec/releases/latest/download/tfsec-linux-amd64 -o tfsec
              chmod +x tfsec
              ./tfsec --no-colour --format json . > tfsec-results.json
              
              echo "Running compliance checks..."
              # Add compliance validation logic here
              
              echo "Infrastructure validation completed"
            EOF
          ]
          
          volume_mount {
            name       = "terraform-config"
            mount_path = "/terraform"
          }
        }
        
        container {
          name  = "policy-validator"
          image = "openpolicyagent/conftest:latest"
          
          command = ["conftest"]
          args = [
            "verify",
            "--policy", "/policies",
            "/terraform/*.tf"
          ]
          
          volume_mount {
            name       = "terraform-config"
            mount_path = "/terraform"
          }
          
          volume_mount {
            name       = "policy-config"
            mount_path = "/policies"
          }
        }
        
        volume {
          name = "terraform-config"
          config_map {
            name = kubernetes_config_map.terraform_validation_config.metadata.0.name
          }
        }
        
        volume {
          name = "policy-config"
          config_map {
            name = kubernetes_config_map.policy_validation_config.metadata.0.name
          }
        }
        
        restart_policy = "Never"
      }
    }
    
    backoff_limit = 3
  }
}

# Terraform Configuration for Validation
resource "kubernetes_config_map" "terraform_validation_config" {
  count = var.enable_validation_pipeline ? 1 : 0
  
  metadata {
    name      = "terraform-validation-config"
    namespace = "argocd"
  }
  
  data = {
    "main.tf" = file("${path.module}/../../main.tf")
    "variables.tf" = file("${path.module}/../../variables.tf")
  }
}

# Policy as Code Configuration
resource "kubernetes_config_map" "policy_validation_config" {
  count = var.enable_validation_pipeline ? 1 : 0
  
  metadata {
    name      = "policy-validation-config"
    namespace = "argocd"
  }
  
  data = {
    "security.rego" = <<EOF
package terraform.security

# Ensure encryption is enabled for storage resources
deny[msg] {
    resource := input.resource_changes[_]
    resource.type == "aws_s3_bucket"
    not resource.change.after.server_side_encryption_configuration
    msg := sprintf("S3 bucket %s must have encryption enabled", [resource.address])
}

# Ensure public access is blocked for S3 buckets
deny[msg] {
    resource := input.resource_changes[_]
    resource.type == "aws_s3_bucket_public_access_block"
    resource.change.after.block_public_acls != true
    msg := sprintf("S3 bucket %s must block public ACLs", [resource.address])
}

# Ensure VPCs have flow logs enabled
deny[msg] {
    vpc := input.resource_changes[_]
    vpc.type == "aws_vpc"
    vpc_id := vpc.change.after.id
    
    flow_log_exists := [fl | fl := input.resource_changes[_]; fl.type == "aws_flow_log"; fl.change.after.vpc_id == vpc_id]
    count(flow_log_exists) == 0
    
    msg := sprintf("VPC %s must have flow logs enabled", [vpc.address])
}
    EOF
    
    "compliance.rego" = <<EOF
package terraform.compliance

# Ensure all resources are properly tagged
required_tags := ["Environment", "Project", "ManagedBy"]

deny[msg] {
    resource := input.resource_changes[_]
    resource.type in ["aws_instance", "aws_s3_bucket", "aws_vpc", "azurerm_virtual_machine", "google_compute_instance"]
    
    missing_tags := [tag | tag := required_tags[_]; not resource.change.after.tags[tag]]
    count(missing_tags) > 0
    
    msg := sprintf("Resource %s is missing required tags: %v", [resource.address, missing_tags])
}

# Ensure backup is configured for critical resources
deny[msg] {
    resource := input.resource_changes[_]
    resource.type in ["aws_db_instance", "azurerm_sql_database"]
    not resource.change.after.backup_retention_period
    msg := sprintf("Database %s must have backup retention configured", [resource.address])
}
    EOF
  }
}

# Drift Detection CronJob
resource "kubernetes_cron_job_v1" "drift_detection" {
  count = var.enable_drift_detection ? 1 : 0
  
  metadata {
    name      = "terraform-drift-detection"
    namespace = "argocd"
  }
  
  spec {
    schedule = var.drift_detection_schedule
    
    job_template {
      metadata {
        labels = {
          app = "drift-detection"
        }
      }
      
      spec {
        template {
          metadata {
            labels = {
              app = "drift-detection"
            }
          }
          
          spec {
            container {
              name  = "terraform-drift"
              image = "hashicorp/terraform:${var.terraform_version}"
              
              command = ["/bin/sh", "-c"]
              args = [
                <<EOF
                  echo "Checking for infrastructure drift..."
                  terraform init
                  terraform plan -detailed-exitcode -out=drift.plan
                  
                  exit_code=$?
                  if [ $exit_code -eq 2 ]; then
                    echo "Infrastructure drift detected!"
                    terraform show -json drift.plan > drift-report.json
                    # Send alert to monitoring system
                    curl -X POST ${var.webhook_url} \
                      -H "Content-Type: application/json" \
                      -d '{"alert":"Infrastructure drift detected","environment":"${var.environment}","timestamp":"$(date -u +%Y-%m-%dT%H:%M:%SZ)"}'
                  else
                    echo "No infrastructure drift detected"
                  fi
                EOF
              ]
              
              env {
                name = "TF_VAR_environment"
                value = var.environment
              }
              
              env {
                name = "AWS_ACCESS_KEY_ID"
                value_from {
                  secret_key_ref {
                    name = "cloud-credentials"
                    key  = "aws_access_key_id"
                  }
                }
              }
              
              env {
                name = "AWS_SECRET_ACCESS_KEY"
                value_from {
                  secret_key_ref {
                    name = "cloud-credentials"
                    key  = "aws_secret_access_key"
                  }
                }
              }
            }
            
            restart_policy = "OnFailure"
          }
        }
        
        backoff_limit = 3
      }
    }
    
    successful_jobs_history_limit = 3
    failed_jobs_history_limit     = 3
  }
}

# Cloud Credentials Secret
resource "kubernetes_secret" "cloud_credentials" {
  metadata {
    name      = "cloud-credentials"
    namespace = "argocd"
  }
  
  data = merge(
    var.enable_aws_deployment ? {
      aws_access_key_id     = var.aws_access_key_id
      aws_secret_access_key = var.aws_secret_access_key
    } : {},
    var.enable_azure_deployment ? {
      azure_client_id       = var.azure_client_id
      azure_client_secret   = var.azure_client_secret
      azure_tenant_id       = var.azure_tenant_id
      azure_subscription_id = var.azure_subscription_id
    } : {},
    var.enable_gcp_deployment ? {
      google_credentials = var.gcp_service_account_key
    } : {}
  )
  
  type = "Opaque"
}

# Monitoring and Alerting for GitOps Pipeline
resource "kubernetes_service_monitor" "argocd_metrics" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "argocd-metrics"
    namespace = "argocd"
    labels = {
      app = "argocd-metrics"
    }
  }
  
  spec {
    selector {
      match_labels = {
        "app.kubernetes.io/name" = "argocd-server-metrics"
      }
    }
    
    endpoints {
      port = "metrics"
      path = "/metrics"
      interval = "30s"
    }
  }
}

# Grafana Dashboard ConfigMap
resource "kubernetes_config_map" "gitops_dashboard" {
  count = var.enable_monitoring ? 1 : 0
  
  metadata {
    name      = "gitops-dashboard"
    namespace = "monitoring"
    labels = {
      grafana_dashboard = "1"
    }
  }
  
  data = {
    "gitops-dashboard.json" = jsonencode({
      dashboard = {
        title = "GitOps Infrastructure Pipeline"
        panels = [
          {
            title = "Deployment Success Rate"
            type  = "stat"
            targets = [{
              expr = "rate(argocd_app_sync_total{phase='Succeeded'}[5m]) / rate(argocd_app_sync_total[5m]) * 100"
            }]
          },
          {
            title = "Infrastructure Drift Events"
            type  = "graph"
            targets = [{
              expr = "increase(terraform_drift_detected_total[1h])"
            }]
          },
          {
            title = "Policy Violations"
            type  = "table"
            targets = [{
              expr = "policy_violations_total"
            }]
          }
        ]
      }
    })
  }
}

# Rollback Mechanism
resource "kubernetes_job" "rollback_mechanism" {
  count = var.enable_rollback_automation ? 1 : 0
  
  metadata {
    name      = "infrastructure-rollback"
    namespace = "argocd"
  }
  
  spec {
    template {
      metadata {
        labels = {
          app = "rollback-mechanism"
        }
      }
      
      spec {
        container {
          name  = "rollback-controller"
          image = "alpine/curl:latest"
          
          command = ["/bin/sh", "-c"]
          args = [
            <<EOF
              echo "Monitoring for failed deployments..."
              while true; do
                # Check ArgoCD application health
                if ! curl -s ${var.argocd_server_url}/api/v1/applications | jq -e '.items[] | select(.status.health.status != "Healthy")'; then
                  echo "Unhealthy application detected, initiating rollback..."
                  
                  # Trigger Terraform Cloud workspace rollback
                  curl -X POST \
                    -H "Authorization: Bearer ${var.terraform_cloud_token}" \
                    -H "Content-Type: application/json" \
                    -d '{"data":{"type":"state-version-outputs","attributes":{"rollback":true}}}' \
                    "https://app.terraform.io/api/v2/workspaces/${tfe_workspace.infrastructure[0].id}/actions/rollback"
                fi
                sleep 300  # Check every 5 minutes
              done
            EOF
          ]
        }
        
        restart_policy = "Always"
      }
    }
  }
}