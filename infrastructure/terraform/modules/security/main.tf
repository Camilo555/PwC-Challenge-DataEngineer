# BMAD Platform - Enterprise Security Hardening and Compliance Automation
# Zero-trust architecture with SOC2, GDPR, HIPAA compliance automation

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
  }
}

locals {
  common_labels = var.tags
  
  # Security policies configuration
  security_policies = {
    network_policies_enabled      = var.enable_network_policies
    pod_security_policies_enabled = var.enable_pod_security_policies
    falco_enabled                = var.enable_falco
    trivy_enabled                = var.enable_trivy
    opa_gatekeeper_enabled       = var.enable_opa_gatekeeper
  }
  
  # Compliance frameworks
  compliance_frameworks = {
    soc2  = var.enable_soc2_controls
    gdpr  = var.enable_gdpr_controls
    hipaa = var.enable_hipaa_controls
  }
  
  # Zero-trust configuration
  zero_trust_config = {
    mtls_enabled           = true
    encryption_at_rest     = true
    encryption_in_transit  = true
    identity_verification  = true
    least_privilege_access = true
  }
}

# Security Namespace
resource "kubernetes_namespace" "security" {
  metadata {
    name = "security-system"
    
    labels = merge(local.common_labels, {
      purpose = "security-hardening"
      compliance = join(",", var.compliance_frameworks)
    })
    
    annotations = {
      "security.bmad.io/hardened" = "true"
      "compliance.bmad.io/frameworks" = join(",", var.compliance_frameworks)
    }
  }
}

# Falco Runtime Security
resource "helm_release" "falco" {
  count = local.security_policies.falco_enabled ? 1 : 0
  
  name       = "falco"
  repository = "https://falcosecurity.github.io/charts"
  chart      = "falco"
  version    = "3.8.4"
  namespace  = kubernetes_namespace.security.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/falco-values.yaml", {
      enable_k8s_audit    = true
      enable_syscalls     = true
      enable_webserver    = true
      webserver_port      = 8765
      grpc_enabled        = true
      grpc_port          = 5060
      json_output        = true
      log_level          = "INFO"
      rules_file         = "/etc/falco/falco_rules.yaml,/etc/falco/k8s_audit_rules.yaml,/etc/falco/rules.d"
      enable_outputs     = true
      slack_webhook      = var.slack_webhook_url
      pagerduty_key      = var.pagerduty_integration_key
    })
  ]
  
  depends_on = [kubernetes_namespace.security]
}

# Trivy Vulnerability Scanner
resource "helm_release" "trivy_operator" {
  count = local.security_policies.trivy_enabled ? 1 : 0
  
  name       = "trivy-operator"
  repository = "https://aquasecurity.github.io/helm-charts"
  chart      = "trivy-operator"
  version    = "0.16.4"
  namespace  = kubernetes_namespace.security.metadata[0].name
  
  values = [
    templatefile("${path.module}/values/trivy-values.yaml", {
      vulnerability_scanner_enabled = true
      config_audit_scanner_enabled  = true
      exposed_secret_scanner_enabled = true
      rbac_assessment_scanner_enabled = true
      infra_assessment_scanner_enabled = true
      compliance_enabled = true
      policy_report_enabled = true
      metrics_enabled = true
      webhook_enabled = true
      webhook_url = var.security_webhook_url
    })
  ]
  
  depends_on = [kubernetes_namespace.security]
}

# OPA Gatekeeper Policy Engine
resource "helm_release" "gatekeeper" {
  count = local.security_policies.opa_gatekeeper_enabled ? 1 : 0
  
  name       = "gatekeeper"
  repository = "https://open-policy-agent.github.io/gatekeeper/charts"
  chart      = "gatekeeper"
  version    = "3.14.0"
  namespace  = "gatekeeper-system"
  
  create_namespace = true
  
  values = [
    templatefile("${path.module}/values/gatekeeper-values.yaml", {
      replicas = 3
      audit_interval = 60
      constraint_violations_limit = 20
      audit_from_cache = false
      emit_admission_events = true
      emit_audit_events = true
      log_level = "INFO"
      enable_external_data = true
      mutation_enabled = true
    })
  ]
}

# Network Security Policies
resource "kubernetes_network_policy" "deny_all_default" {
  count = local.security_policies.network_policies_enabled ? 1 : 0
  
  metadata {
    name      = "deny-all-default"
    namespace = "default"
  }

  spec {
    pod_selector {}
    policy_types = ["Ingress", "Egress"]
  }
}

resource "kubernetes_network_policy" "allow_dns" {
  count = local.security_policies.network_policies_enabled ? 1 : 0
  
  metadata {
    name      = "allow-dns"
    namespace = "default"
  }

  spec {
    pod_selector {}
    policy_types = ["Egress"]
    
    egress {
      to {
        namespace_selector {
          match_labels = {
            name = "kube-system"
          }
        }
      }
      
      ports {
        protocol = "UDP"
        port     = "53"
      }
      
      ports {
        protocol = "TCP"
        port     = "53"
      }
    }
  }
}

resource "kubernetes_network_policy" "bmad_api_network_policy" {
  count = local.security_policies.network_policies_enabled ? 1 : 0
  
  metadata {
    name      = "bmad-api-network-policy"
    namespace = "default"
  }

  spec {
    pod_selector {
      match_labels = {
        app = "bmad-api"
      }
    }
    
    policy_types = ["Ingress", "Egress"]
    
    ingress {
      from {
        pod_selector {
          match_labels = {
            app = "bmad-frontend"
          }
        }
      }
      
      from {
        pod_selector {
          match_labels = {
            app = "bmad-mobile"
          }
        }
      }
      
      from {
        namespace_selector {
          match_labels = {
            name = "istio-system"
          }
        }
      }
      
      ports {
        protocol = "TCP"
        port     = "8080"
      }
    }
    
    egress {
      to {
        pod_selector {
          match_labels = {
            app = "postgresql"
          }
        }
      }
      
      ports {
        protocol = "TCP"
        port     = "5432"
      }
    }
    
    egress {
      to {
        pod_selector {
          match_labels = {
            app = "redis"
          }
        }
      }
      
      ports {
        protocol = "TCP"
        port     = "6379"
      }
    }
  }
}

# Pod Security Standards
resource "kubernetes_config_map" "pod_security_standards" {
  count = local.security_policies.pod_security_policies_enabled ? 1 : 0
  
  metadata {
    name      = "pod-security-standards"
    namespace = kubernetes_namespace.security.metadata[0].name
  }

  data = {
    "pod-security-policy.yaml" = templatefile("${path.module}/templates/pod-security-policy.yaml.tpl", {
      allow_privilege_escalation = false
      allow_privileged          = false
      required_drop_capabilities = ["ALL"]
      allowed_capabilities      = []
      allowed_host_paths        = []
      allowed_flex_volumes      = []
      forbidden_sysctls         = ["*"]
      fs_group_rule            = "RunAsAny"
      run_as_user_rule         = "MustRunAsNonRoot"
      supplemental_groups_rule  = "RunAsAny"
      se_linux_rule            = "RunAsAny"
    })
  }
}

# Security Scanning CronJob
resource "kubernetes_cron_job" "security_scan" {
  metadata {
    name      = "security-scan"
    namespace = kubernetes_namespace.security.metadata[0].name
  }
  
  spec {
    concurrency_policy = "Forbid"
    schedule          = "0 2 * * *"  # Daily at 2 AM
    
    job_template {
      metadata {
        name = "security-scan"
      }
      
      spec {
        template {
          metadata {
            name = "security-scan"
          }
          
          spec {
            restart_policy = "OnFailure"
            
            container {
              name  = "security-scanner"
              image = "bmad/security-scanner:v1.0.0"
              
              command = ["/bin/sh"]
              args = [
                "-c",
                "trivy fs --security-checks vuln,config,secret,license /app && trivy k8s --report summary cluster"
              ]
              
              env {
                name  = "TRIVY_DB_REPOSITORY"
                value = "ghcr.io/aquasecurity/trivy-db"
              }
              
              env {
                name  = "TRIVY_JAVA_DB_REPOSITORY"
                value = "ghcr.io/aquasecurity/trivy-java-db"
              }
              
              env {
                name  = "SCAN_RESULTS_WEBHOOK"
                value = var.security_webhook_url
              }
              
              resources {
                requests = {
                  cpu    = "100m"
                  memory = "256Mi"
                }
                limits = {
                  cpu    = "500m"
                  memory = "1Gi"
                }
              }
              
              volume_mount {
                name       = "docker-socket"
                mount_path = "/var/run/docker.sock"
                read_only  = true
              }
            }
            
            volume {
              name = "docker-socket"
              host_path {
                path = "/var/run/docker.sock"
              }
            }
            
            service_account_name = kubernetes_service_account.security_scanner.metadata[0].name
          }
        }
      }
    }
  }
}

# Service Account for Security Scanner
resource "kubernetes_service_account" "security_scanner" {
  metadata {
    name      = "security-scanner"
    namespace = kubernetes_namespace.security.metadata[0].name
  }
}

# ClusterRole for Security Scanner
resource "kubernetes_cluster_role" "security_scanner" {
  metadata {
    name = "security-scanner"
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "nodes", "namespaces", "configmaps", "secrets", "services"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "replicasets", "daemonsets", "statefulsets"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["batch"]
    resources  = ["jobs", "cronjobs"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["networking.k8s.io"]
    resources  = ["networkpolicies", "ingresses"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["rbac.authorization.k8s.io"]
    resources  = ["roles", "rolebindings", "clusterroles", "clusterrolebindings"]
    verbs      = ["get", "list", "watch"]
  }
}

# ClusterRoleBinding for Security Scanner
resource "kubernetes_cluster_role_binding" "security_scanner" {
  metadata {
    name = "security-scanner"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.security_scanner.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.security_scanner.metadata[0].name
    namespace = kubernetes_namespace.security.metadata[0].name
  }
}

# Secret Management
resource "kubernetes_secret" "security_credentials" {
  metadata {
    name      = "security-credentials"
    namespace = kubernetes_namespace.security.metadata[0].name
    
    annotations = {
      "secret.bmad.io/encrypted" = "true"
      "secret.bmad.io/compliance" = "SOC2,GDPR,HIPAA"
    }
  }
  
  data = {
    falco-webhook-url = var.slack_webhook_url
    pagerduty-key    = var.pagerduty_integration_key
    security-webhook = var.security_webhook_url
  }
  
  type = "Opaque"
}

# Compliance Monitoring Deployment
resource "kubernetes_deployment" "compliance_monitor" {
  metadata {
    name      = "compliance-monitor"
    namespace = kubernetes_namespace.security.metadata[0].name
    
    labels = {
      app = "compliance-monitor"
    }
  }

  spec {
    replicas = 2

    selector {
      match_labels = {
        app = "compliance-monitor"
      }
    }

    template {
      metadata {
        labels = {
          app = "compliance-monitor"
        }
        
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
          "prometheus.io/path"   = "/metrics"
        }
      }

      spec {
        container {
          image = "bmad/compliance-monitor:v1.0.0"
          name  = "compliance-monitor"
          
          port {
            container_port = 8080
            name          = "http"
          }
          
          env {
            name  = "COMPLIANCE_FRAMEWORKS"
            value = join(",", var.compliance_frameworks)
          }
          
          env {
            name  = "SCAN_INTERVAL"
            value = "3600"  # 1 hour
          }
          
          env {
            name  = "SOC2_CONTROLS_ENABLED"
            value = tostring(local.compliance_frameworks.soc2)
          }
          
          env {
            name  = "GDPR_CONTROLS_ENABLED"
            value = tostring(local.compliance_frameworks.gdpr)
          }
          
          env {
            name  = "HIPAA_CONTROLS_ENABLED"
            value = tostring(local.compliance_frameworks.hipaa)
          }
          
          env {
            name = "WEBHOOK_URL"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.security_credentials.metadata[0].name
                key  = "security-webhook"
              }
            }
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
            name       = "compliance-config"
            mount_path = "/etc/compliance"
            read_only  = true
          }
        }
        
        volume {
          name = "compliance-config"
          config_map {
            name = kubernetes_config_map.compliance_config.metadata[0].name
          }
        }
        
        service_account_name = kubernetes_service_account.compliance_monitor.metadata[0].name
      }
    }
  }
}

# Compliance Configuration
resource "kubernetes_config_map" "compliance_config" {
  metadata {
    name      = "compliance-config"
    namespace = kubernetes_namespace.security.metadata[0].name
  }

  data = {
    "soc2-controls.yaml" = templatefile("${path.module}/templates/soc2-controls.yaml.tpl", {
      enable_cc6_1 = true  # Logical and physical access controls
      enable_cc6_2 = true  # System access management
      enable_cc6_3 = true  # Network access controls
      enable_cc7_1 = true  # Security monitoring and detection
    })
    
    "gdpr-controls.yaml" = templatefile("${path.module}/templates/gdpr-controls.yaml.tpl", {
      enable_article_25 = true  # Data protection by design
      enable_article_32 = true  # Security of processing
      enable_article_33 = true  # Data breach notification
      enable_article_35 = true  # Data protection impact assessment
    })
    
    "hipaa-controls.yaml" = templatefile("${path.module}/templates/hipaa-controls.yaml.tpl", {
      enable_164_308 = true  # Administrative safeguards
      enable_164_310 = true  # Physical safeguards
      enable_164_312 = true  # Technical safeguards
      enable_164_314 = true  # Organizational requirements
    })
  }
}

# Service Account for Compliance Monitor
resource "kubernetes_service_account" "compliance_monitor" {
  metadata {
    name      = "compliance-monitor"
    namespace = kubernetes_namespace.security.metadata[0].name
  }
}

# ClusterRole for Compliance Monitor
resource "kubernetes_cluster_role" "compliance_monitor" {
  metadata {
    name = "compliance-monitor"
  }

  rule {
    api_groups = [""]
    resources  = ["*"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["apps", "extensions", "batch", "networking.k8s.io", "rbac.authorization.k8s.io"]
    resources  = ["*"]
    verbs      = ["get", "list", "watch"]
  }
  
  rule {
    api_groups = ["security.istio.io", "networking.istio.io", "config.istio.io"]
    resources  = ["*"]
    verbs      = ["get", "list", "watch"]
  }
}

# ClusterRoleBinding for Compliance Monitor
resource "kubernetes_cluster_role_binding" "compliance_monitor" {
  metadata {
    name = "compliance-monitor"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.compliance_monitor.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.compliance_monitor.metadata[0].name
    namespace = kubernetes_namespace.security.metadata[0].name
  }
}

# Service for Compliance Monitor
resource "kubernetes_service" "compliance_monitor" {
  metadata {
    name      = "compliance-monitor"
    namespace = kubernetes_namespace.security.metadata[0].name
    
    labels = {
      app = "compliance-monitor"
    }
  }
  
  spec {
    selector = {
      app = "compliance-monitor"
    }
    
    port {
      port        = 8080
      target_port = 8080
      protocol    = "TCP"
    }
    
    type = "ClusterIP"
  }
}

# Cert-Manager for TLS Certificate Management
resource "helm_release" "cert_manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  version    = "v1.13.2"
  namespace  = "cert-manager"
  
  create_namespace = true
  
  set {
    name  = "installCRDs"
    value = "true"
  }
  
  set {
    name  = "global.podSecurityPolicy.enabled"
    value = "true"
  }
  
  set {
    name  = "prometheus.enabled"
    value = "true"
  }
  
  depends_on = [kubernetes_namespace.security]
}

# ClusterIssuer for Let's Encrypt
resource "kubernetes_manifest" "letsencrypt_prod" {
  manifest = {
    apiVersion = "cert-manager.io/v1"
    kind       = "ClusterIssuer"
    metadata = {
      name = "letsencrypt-prod"
    }
    spec = {
      acme = {
        server = "https://acme-v02.api.letsencrypt.org/directory"
        email  = var.alert_email
        privateKeySecretRef = {
          name = "letsencrypt-prod"
        }
        solvers = [
          {
            http01 = {
              ingress = {
                class = "nginx"
              }
            }
          }
        ]
      }
    }
  }
  
  depends_on = [helm_release.cert_manager]
}

# Security Monitoring Service Monitor
resource "kubernetes_manifest" "security_service_monitor" {
  manifest = {
    apiVersion = "monitoring.coreos.com/v1"
    kind       = "ServiceMonitor"
    metadata = {
      name      = "security-monitoring"
      namespace = kubernetes_namespace.security.metadata[0].name
      labels = {
        app = "security-monitoring"
      }
    }
    spec = {
      selector = {
        matchLabels = {
          app = "compliance-monitor"
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